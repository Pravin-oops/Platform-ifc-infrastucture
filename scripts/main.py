"""ECS / CLI entry point for the IFC trigger connector.

    python ifc_trigger_connector/scripts/main.py --config s3://.../connector_config.yaml

Also exposes two subcommands that are useful without a BSP connection:

    python ifc_trigger_connector/scripts/main.py validate --input samples/trigger_events.jsonl
    python ifc_trigger_connector/scripts/main.py catalogue

The process exit code is the catalogue exit code for whatever scenario ended the
run, so ECS's stopped task record identifies the failure without log archaeology.
"""

from __future__ import annotations

import argparse
import json





import logging
import os
import sys
from typing import List, Optional

# Runnable directly (`python .../scripts/main.py`) as well as on PYTHONPATH: put
# the directory that holds the ifc_trigger_connector package on sys.path.
sys.path.insert(
    0,
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
)

from ifc_trigger_connector.utility import failure_catalog as catalog
from ifc_trigger_connector.utility.error_classifier import ConnectorError, RecordRejected, classify
from ifc_trigger_connector.utility.health_utility import HealthServer, HealthState
from ifc_trigger_connector.utility.observability_utility import Metrics, configure_logging
from ifc_trigger_connector.utility.resilience_utility import ShutdownSignal
from ifc_trigger_connector.utility.connector_config import load_settings

logger = logging.getLogger("ifc_trigger_connector")

DEFAULT_CONFIG_ENV = "APP_CONFIG_PATH"
DEFAULT_ENVELOPE_DUMP = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "samples",
    "outbound",
    "validated_envelopes.json",
)


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="ifc-connector", description="IFC trigger connector")
    parser.add_argument(
        "--config",
        default=os.environ.get(DEFAULT_CONFIG_ENV),
        help="Connector config YAML (local path or s3://). Defaults to $APP_CONFIG_PATH.",
    )
    parser.add_argument("--log-level", default=None, help="Override app.log_level.")

    sub = parser.add_subparsers(dest="command")

    validate = sub.add_parser(
        "validate", help="Build and validate trigger envelopes from a file, without publishing."
    )
    validate.add_argument("--input", required=True, help="Trigger event JSON / JSONL file.")
    validate.add_argument("--show-payload", action="store_true", help="Print the built payload.")
    validate.add_argument(
        "--save-envelopes",
        nargs="?",
        const=DEFAULT_ENVELOPE_DUMP,
        default=None,
        metavar="PATH",
        help=f"Write the pre-serialization Avro envelopes as JSON (default: {DEFAULT_ENVELOPE_DUMP}).",
    )

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


def _command_validate(path: str, *, show_payload: bool, save_envelopes: Optional[str] = None) -> int:
    """Offline contract check: does this file produce publishable envelopes?

    Uses the bundled .avsc, so it needs neither AWS nor BSP. This is the check
    to run in CI against sample data whenever a trigger definition changes.
    """
    from ifc_trigger_connector.utility.connector_utility import load_schema_document
    from ifc_trigger_connector.utility.connector_config import SchemaRegistrySettings
    from ifc_trigger_connector.utility.trigger_source import ParseFailure, TriggerSource
    from ifc_trigger_connector.utility.connector_config import SourceSettings
    from ifc_trigger_connector.utility.tb_outcome_schema import EnvelopeBuilder

    schema = load_schema_document(SchemaRegistrySettings(mode="DEV").schema_path)
    builder = EnvelopeBuilder(avro_schema=schema)

    source = TriggerSource(SourceSettings(type="local", path=path))
    ok = failed = 0
    envelopes: List[dict] = []

    for item in source.stream(limit=10_000):
        if isinstance(item, ParseFailure):
            failed += 1
            print(f"PARSE FAIL  {item.source_object}[{item.index}]: {item.error}")
            continue

        try:
            built = builder.build(item)
        except RecordRejected as exc:
            failed += 1
            print(f"REJECTED    {item.trigger_sub_type} {item.csid}: {exc}")
            print(f"            detail: {json.dumps(exc.detail, default=str)}")
            continue

        ok += 1
        print(f"OK          {built.trigger_id}  seq={built.record['sequenceNumber']}")
        if show_payload:
            print(json.dumps(json.loads(built.record["payload"]), indent=2))
        if save_envelopes:
            envelopes.append(built.record)

    # The record dict is exactly what AvroSerializer receives, so this file is
    # the envelope as it stands immediately before serialization.
    if save_envelopes:
        os.makedirs(os.path.dirname(os.path.abspath(save_envelopes)), exist_ok=True)
        with open(save_envelopes, "w", encoding="utf-8") as handle:
            json.dump(envelopes, handle, indent=2, default=str)
            handle.write("\n")
        print(f"\nSaved {len(envelopes)} envelope(s) to {save_envelopes}")

    print(f"\n{ok} valid, {failed} rejected")
    return 0 if failed == 0 else catalog.SCHEMA_VALIDATION_FAILURE.exit_code


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)

    if args.command == "catalogue":
        configure_logging("WARNING")
        return _command_catalogue()

    if args.command == "validate":
        configure_logging(args.log_level or "INFO")
        return _command_validate(
            args.input, show_payload=args.show_payload, save_envelopes=args.save_envelopes
        )

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
    except Exception as exc:
        classification = classify(exc, operation="load_settings")
        logger.error(
            "Configuration could not be loaded",
            extra={"config_path": args.config, **classification.to_dict()},
        )
        return catalog.CONTAINER_FAILURE.exit_code

    configure_logging(args.log_level or settings.app.log_level)

    from ifc_trigger_connector.utility.connector_runner import ConnectorRunner

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
