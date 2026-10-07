"""ECS / CLI entry point for the IFC trigger connector."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

# Runnable directly (`python .../scripts/main.py`) from any working directory:
# put the app root - the directory that holds utility/ and scripts/ - on sys.path.
APP_ROOT = Path(__file__).resolve().parent.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

# Pick up APP_CONFIG_PATH and the IFC_ overlay from <app root>/.env when one is
# present. Variables already set (e.g. by the ECS task definition) always win.
load_dotenv(APP_ROOT / ".env", override=False)

from utility import failure_catalog as catalog
from utility.error_classifier import ConnectorError, classify
from utility.health_utility import HealthServer, HealthState
from utility.observability_utility import Metrics, configure_logging
from utility.resilience_utility import ShutdownSignal
from utility.connector_config import load_settings

logger = logging.getLogger("ifc_trigger_connector")

DEFAULT_CONFIG_ENV = "APP_CONFIG_PATH"


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="ifc-connector", description="IFC trigger connector")
    parser.add_argument(
        "--config",
        default=os.environ.get(DEFAULT_CONFIG_ENV),
        help="Connector config YAML (local path or s3://). Defaults to $APP_CONFIG_PATH.",
    )
    parser.add_argument("--log-level", default=None, help="Override app.log_level.")

    sub = parser.add_subparsers(dest="command")
    sub.add_parser("catalogue", help="Print the failure scenario catalogue as JSON.")

    return parser.parse_args(argv)


def _command_catalogue() -> int:
    print(
        json.dumps(
            {
                "scenarios": [s.to_dict() for s in catalog.SCENARIOS.values()],
                "exit_codes": {
                    "success": catalog.EXIT_OK,
                    "work_remaining": catalog.EXIT_WORK_REMAINING,
                    **{s.key: s.exit_code for s in catalog.SCENARIOS.values()},
                },
            },
            indent=2,
        )
    )
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)

    if args.command == "catalogue":
        configure_logging("WARNING")
        return _command_catalogue()

    if not args.config:
        print(
            f"--config is required (or set {DEFAULT_CONFIG_ENV})",
            file=sys.stderr,
        )
        return catalog.CONTAINER_FAILURE.exit_code

    # Logging is configured twice on purpose: once to capture config-loading
    # failures, then again at the level the config asks for.
    configure_logging(args.log_level or "INFO")

    try:
        settings = load_settings(args.config)
        # IFC_RUN__TRIGGER decides which Athena table this run reads.
        settings.select_trigger(None)
    except Exception as exc:
        classification = classify(exc, operation="load_settings")
        logger.error(
            "Configuration could not be loaded",
            extra={"config_path": args.config, **classification.to_dict()},
        )
        return catalog.CONTAINER_FAILURE.exit_code

    configure_logging(args.log_level or settings.app.log_level)

    from utility.connector_runner import ConnectorRunner

    shutdown = ShutdownSignal().install()
    health = HealthState()
    metrics = Metrics(
        dimensions={
            "Environment": settings.app.environment,
            "Application": settings.app.name,
            "Topic": settings.kafka.topic,
        }
    )

    server: Optional[HealthServer] = None
    if settings.health.enabled:
        server = HealthServer(
            health,
            host=settings.health.bind_host,
            port=settings.health.port,
            metrics_provider=metrics.snapshot,
        ).start()

    try:
        runner = ConnectorRunner(settings, shutdown=shutdown, health=health, metrics=metrics)
        return runner.run()
    except ConnectorError as exc:
        logger.error("Connector failed to start", extra={"scenario": exc.scenario.key})
        return exc.exit_code
    except Exception:
        logger.exception("Connector failed to start")
        return catalog.CONTAINER_FAILURE.exit_code
    finally:
        if server is not None:
            server.stop()


if __name__ == "__main__":
    sys.exit(main())
