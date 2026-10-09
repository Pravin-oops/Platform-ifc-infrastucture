"""ECS entry point for the IFC trigger connector."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

from dotenv import load_dotenv
load_dotenv()
# Runnable directly from any working directory: put the app root - the directory
# that holds utility/ and scripts/ - on sys.path.
APP_ROOT = Path(__file__).resolve().parent.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

# Pick up the IFC_ overlay from <app root>/.env when one is present.
# Variables injected by the ECS task definition always win.
load_dotenv(APP_ROOT / ".env", override=False)

from utility import failure_catalog as catalog
from utility.audit_utility import new_run_id, write_invocation_manifest
from utility.connector_config import ENVIRONMENT_VARIABLE, config_path_for, load_settings
from utility.error_classifier import ConnectorError, classify
from utility.failure_notifier import Notifier
from utility.health_utility import HealthServer, HealthState
from utility.observability_utility import Metrics, configure_logging
from utility.resilience_utility import ShutdownSignal
from utility.tb_outcome_schema import now_timestamp
from utility.trigger_definitions import resolve as resolve_trigger
from utility import recon_gate
from utility import run_gate
from utility.run_gate import RunMarker, execution_date, month_of, should_run
from utility.trigger_batch_notifier import (
    TriggerBatchNotification,
    TriggerBatchNotifier,
)

logger = logging.getLogger("ifc_trigger_connector.ecs")

def ecs_task_metadata() -> Dict[str, Optional[str]]:
    """Identity of this task, for the run manifest and for alert routing."""
    return {
        "cluster": os.getenv("ECS_CLUSTER"),
        "task_arn": os.getenv("ECS_TASK_ARN"),
        "container_name": os.getenv("ECS_CONTAINER_NAME"),
        "metadata_uri": os.getenv("ECS_CONTAINER_METADATA_URI_V4"),
        "image_tag": os.getenv("IMAGE_TAG"),
    }


def _load(event: Dict[str, Any]):
    """Load the environment's config, set the log level, return (settings, config_path)."""
    # Configured twice on purpose: once so a config-loading failure is logged in
    # the right format, then again at the level the config asks for.
    configure_logging(os.getenv("IFC_LOG_LEVEL", "INFO"))
    config_path = config_path_for(os.getenv(ENVIRONMENT_VARIABLE))
    settings = load_settings(config_path)
    # The trigger picks the Athena table, so it is required before anything else runs.
    settings.select_trigger(event.get("trigger"))
    configure_logging(os.getenv("IFC_LOG_LEVEL") or settings.app.log_level)

    return settings, config_path


class Invocation:
    """One ECS invocation, so a run that stops before the runner still leaves a
    manifest in the audit bucket. The runner writes its own once it is reached."""

    def __init__(self, settings):
        self.settings = settings
        self.started_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        self._started = time.perf_counter()

    def record(self, stage: str, outcome: str, exit_code: int, *, reason: Optional[str] = None,
               classification=None, gate: Optional[Dict[str, Any]] = None) -> str:
        return write_invocation_manifest(
            self.settings,
            stage=stage,
            outcome=outcome,
            exit_code=exit_code,
            started_at=self.started_at,
            duration_seconds=time.perf_counter() - self._started,
            reason=reason,
            classification=classification,
            gate=gate,
        )


class Gate(NamedTuple):
    """A claimed gate: which trigger this run is for, and where to record it."""

    trigger: str
    marker: RunMarker


def _gate(settings, event: Dict[str, Any]) -> Tuple[Optional[Gate], Optional[str]]:
    """Decide whether to process. Returns (gate, skip_reason)."""
    if not settings.gate_active:
        return None, None

    gate = Gate(settings.trigger, RunMarker(settings.run_marker.path))
    outcome = should_run(
        gate.trigger,
        gate.marker,
        month=month_of(execution_date()),
        force=event.get("force") or settings.run.force,
    )
    logger.info("Run gate: %s - %s", "PROCEED" if outcome.proceed else "SKIP", outcome.reason)

    if outcome.mark_not_ran:
        _record(gate, run_gate.STATUS_NOT_RAN, reason=outcome.reason)

    return gate, None if outcome.proceed else outcome.reason


def _record(gate: Optional[Gate], status: str, *, records: int = 0, reason: str = "") -> None:
    """Append this invocation's outcome to the run marker file."""
    if gate is None:
        return
    month = month_of(execution_date())
    try:
        gate.marker.record(gate.trigger, month, status=status, records=records, reason=reason)
        logger.info(
            "Run marker recorded",
            extra={
                "trigger": gate.trigger,
                "year_month": month,
                "run_status": status,
                "records_processed": records,
                "reason": reason,
            },
        )
    except Exception:
        logger.exception(
            "Could not record the run marker; the next invocation may repeat this month",
            extra={"trigger": gate.trigger, "year_month": month, "run_status": status},
        )


def _recon_gate(settings, config_path: str, task: Dict[str, Optional[str]],
                invocation: Optional[Invocation] = None):
    """Upstream reconciliation check. Returns a summary when the run stops here."""
    if not settings.recon_active:
        logger.info(
            "Upstream reconciliation gate is not active",
            extra={"enabled": settings.recon.enabled, "trigger": settings.run.trigger},
        )
        return None

    month = month_of(execution_date())
    # Upstream stamps recon rows with the current month, even when reprocessing.
    recon_month = month_of(run_gate.today())
    try:
        decision = recon_gate.evaluate(
            settings.recon, settings.source.athena, execution_month=recon_month
        )
    except Exception as exc:
        # Never let the gate itself decide the run by accident: an unexpected
        # error here is a failure to classify, not permission to publish.
        logger.exception("Upstream reconciliation gate could not be evaluated")
        classification = classify(exc, operation="recon_gate", topic=settings.kafka.topic)
        _notify(settings, classification, config_path, task)
        summary = {
            "exit_code": classification.exit_code,
            "outcome": "UPSTREAM_RECON_UNREADABLE",
            "reason": str(exc),
            "scenario": classification.scenario.key,
            "execution_month": month,
            "recon_month": recon_month,
            "config_path": config_path,
            "ecs": task,
        }
        if invocation is not None:
            summary["run_id"] = invocation.record(
                "RECON_GATE", summary["outcome"], summary["exit_code"],
                reason=summary["reason"], classification=classification,
                gate={"execution_month": month, "recon_month": recon_month},
            )
        return summary

    if decision.proceed:
        logger.info(
            "Upstream reconciliation passed",
            extra={"execution_month": month, "recon_month": recon_month, **decision.to_dict()},
        )
        return None

    if decision.no_data:
        return _no_data_month(settings, decision, month, config_path, task, invocation)

    logger.error(
        "Upstream reconciliation blocked the run: %s",
        decision.reason,
        extra={"execution_month": month, "recon_month": recon_month, **decision.to_dict()},
    )
    classification = classify(
        ConnectorError(decision.reason, decision.blocking_scenario),
        operation="recon_gate",
        topic=settings.kafka.topic,
    )
    _notify(settings, classification, config_path, task)
    summary = {
        "exit_code": decision.exit_code,
        "outcome": decision.outcome,
        "reason": decision.reason,
        "scenario": decision.scenario_key,
        "execution_month": month,
        "config_path": config_path,
        "recon": decision.to_dict(),
        "ecs": task,
    }
    if invocation is not None:
        summary["run_id"] = invocation.record(
            "RECON_GATE", decision.outcome, decision.exit_code,
            reason=decision.reason, classification=classification,
            gate={"execution_month": month, **decision.to_dict()},
        )
    return summary


def _no_data_month(settings, decision, month: str, config_path: str,
                   task: Dict[str, Optional[str]],
                   invocation: Optional[Invocation]) -> Dict[str, Any]:
    """A genuine empty month: nothing to publish, and the month is delivered."""
    logger.info(
        "Upstream reconciled zero records; nothing to publish this month",
        extra={"execution_month": month, **decision.to_dict()},
    )
    summary: Dict[str, Any] = {
        "exit_code": catalog.EXIT_OK,
        "outcome": decision.outcome,
        "reason": decision.reason,
        "execution_month": month,
        "run_status": run_gate.STATUS_SUCCESS,
        "month_delivered": month,
        "config_path": config_path,
        "recon": decision.to_dict(),
        "ecs": task,
    }
    if invocation is not None:
        summary["run_id"] = invocation.record(
            "RECON_GATE", decision.outcome, catalog.EXIT_OK, reason=decision.reason,
            gate={"execution_month": month, **decision.to_dict()},
        )
    try:
        summary["sns_batch_notification"] = _publish_zero_batch_notification(
            settings, summary.get("run_id") or new_run_id()
        )
    except Exception as exc:
        summary["sns_batch_notification"] = _sns_failed(exc)
    return summary


def _failure_summary(exc, settings, config_path, task,
                     invocation: Optional[Invocation] = None) -> Dict[str, Any]:
    """Classify, alert and summarise a failure that never reached the runner."""
    classification = classify(exc, operation="ecs_handler", topic=settings.kafka.topic)
    _notify(settings, classification, config_path, task)
    run_id = (
        invocation.record("STARTUP", "FAILED", classification.exit_code,
                          reason=str(exc), classification=classification)
        if invocation is not None
        else None
    )
    return {
        "run_id": run_id,
        "exit_code": classification.exit_code,
        "scenario": classification.scenario.key,
        "config_path": config_path,
        "ecs": task,
    }


def ecs_handler(event: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Run one connector lifecycle and return its summary."""
    event = event or {}
    settings, config_path = _load(event)
    task = ecs_task_metadata()
    invocation = Invocation(settings)

    if settings.run.month:
        logger.warning(
            "Reprocessing %s: run.month overrides the current month for the Athena "
            "business_date, the business month and the run marker; the recon check "
            "still needs a recon row from the current month",
            settings.run.month,
            extra={"run_month": settings.run.month, "force": settings.run.force},
        )

    registry = settings.schema_registry
    logger.info(
        "Connector starting on ECS: trigger=%s table=%s topic=%s schema_registry_mode=%s "
        "schema_id=%s kafka_connection=%s environment=%s",
        settings.run.trigger,
        settings.source.table,
        settings.kafka.topic,
        registry.mode,
        # SECURE resolves the id from the registry at startup; the
        # "Kafka target ready" line reports what it resolved to.
        registry.schema_id if registry.mode == "DEV" and registry.schema_id else "from registry",
        "BSP" if settings.kafka.bsp_config_path else "direct",
        settings.app.environment,
        extra={
            "config_path": config_path,
            "topic": settings.kafka.topic,
            "trigger": settings.run.trigger,
            "schema_registry_mode": registry.mode,
            "schema_registry_url": registry.url,
            "configured_schema_id": registry.schema_id,
            "source_table": settings.source.table,
            "run_month": month_of(execution_date()),
            **{k: v for k, v in task.items() if v},
        },
    )

    try:
        gate, skip_reason = _gate(settings, event)
    except Exception as exc:
        # The run marker file could not be read: still an invocation, so it is
        # recorded before the failure propagates.
        invocation.record(
            "RUN_GATE", "FAILED", catalog.CONTAINER_FAILURE.exit_code, reason=str(exc),
            classification=classify(exc, operation="run_gate", topic=settings.kafka.topic),
        )
        raise

    if skip_reason:
        return {
            "run_id": invocation.record(
                "RUN_GATE", "SKIPPED", catalog.EXIT_OK, reason=skip_reason,
                gate={"trigger": gate.trigger if gate else settings.run.trigger},
            ),
            "exit_code": catalog.EXIT_OK,
            "outcome": "SKIPPED",
            "reason": skip_reason,
            "ecs": task,
        }

    # After the run gate's skips, so skipped days never alert; before preflight and the
    # marker claim, so a blocked run costs only a container start.
    blocked = _recon_gate(settings, config_path, task, invocation)
    if blocked is not None:
        if blocked.get("outcome") == recon_gate.OUTCOME_NO_DATA:
            # A genuine empty month is delivered: SUCCESS with zero records, so
            # the rest of the window stands down instead of announcing it again.
            _record(gate, run_gate.STATUS_SUCCESS, records=0, reason=recon_gate.OUTCOME_NO_DATA)
        else:
            # Recorded, so the file shows upstream was not ready rather than nothing.
            _record(gate, run_gate.STATUS_NOT_RAN, reason=str(blocked.get("outcome")))
        return blocked

    # Installed before the runner is built: a deployment can stop the task while
    # preflight is still running, and that drain must still be orderly.
    shutdown = ShutdownSignal().install()
    health = HealthState()
    metrics = Metrics(
        dimensions={
            "Environment": settings.app.environment,
            "Application": settings.app.name,
            "Topic": settings.kafka.topic,
        }
    )
    server = (
        HealthServer(
            health,
            host=settings.health.bind_host,
            port=settings.health.port,
            metrics_provider=metrics.snapshot,
        ).start()
        if settings.health.enabled
        else None
    )

    # Set once runner.run() is entered: from then on the runner writes the
    # manifest itself, whatever happens, so a later failure must not add another.
    runner_started = False
    try:
        from utility.connector_runner import ConnectorRunner

        runner = ConnectorRunner(settings, shutdown=shutdown, health=health, metrics=metrics)
        runner_started = True
        exit_code = runner.run()

        try:
            sns_status = _publish_batch_notification(settings, runner)
        except Exception as exc:
            sns_status = _sns_failed(exc)

        last = runner.last_result
        summary = {
            "run_id": runner.run_id,
            "exit_code": exit_code,
            "outcome": last.outcome if last else "FAILED",
            "topic": settings.kafka.topic,
            "schema_registry_mode": settings.schema_registry.mode,
            # getattr: reporting must never be what fails a delivered run.
            "schema_id": getattr(runner, "schema_id", None),
            "published": last.counters.published if last else 0,
            "acked": last.counters.acked if last else 0,
            "delivery_failed": last.counters.delivery_failed if last else 0,
            "quarantined": last.counters.quarantined if last else 0,
            "sns_batch_notification": sns_status,
            "config_path": config_path,
            "environment": settings.app.environment,
            # The source details, so the stopped-task record says what this run read.
            "source": runner.last_result.source if runner.last_result else {"table": settings.source.table},
            "ecs": task,
        }

        # Only a delivered run closes the month; anything else is FAILURE and retries.
        delivered = runner.last_result.counters.acked if runner.last_result else 0
        month = month_of(execution_date())
        if exit_code == catalog.EXIT_OK and delivered:
            _record(gate, run_gate.STATUS_SUCCESS, records=delivered)
            summary["month_delivered"] = month
        else:
            _record(
                gate,
                run_gate.STATUS_FAILURE,
                records=delivered,
                reason=f"exit_code={exit_code}",
            )
        summary["run_status"] = (
            run_gate.STATUS_SUCCESS
            if exit_code == catalog.EXIT_OK and delivered
            else run_gate.STATUS_FAILURE
        )

        return summary

    except Exception as exc:
        logger.exception("Connector failed to start")
        # A startup failure is still an invocation that ran and did not deliver.
        _record(gate, run_gate.STATUS_FAILURE, reason="startup failure")
        return _failure_summary(
            exc, settings, config_path, task, None if runner_started else invocation
        )

    finally:
        if server is not None:
            server.stop()


def _publish_batch_notification(
    settings,
    runner,
) -> Dict[str, Any]:
    """Publish successful Trigger Backbone batch notification."""

    result = runner.last_result

    skip_reason = _batch_notification_skip_reason(settings, result)
    if skip_reason:
        _log_batch_skip(settings, skip_reason, runner.run_id)
        return {"status": "SKIPPED", "reason": skip_reason}

    notification = TriggerBatchNotification(
        Trigger_Originating_BU=
            settings.notifications.trigger_originating_bu,

        No_Of_Messages_Produced=
            result.counters.acked,

        Trigger_Sub_Type=
            result.published_trigger_subtype,

        Topic_Name=
            settings.kafka.topic,

        Trigger_Batch_Start_Timestamp=
            result.batch_start_timestamp,

        Trigger_Batch_End_Timestamp=
            result.batch_end_timestamp,

        Event_Timestamp=
            now_timestamp(),

        Correlation_Id=
            runner.run_id,
    )

    # The notifier logs the topic, full message and MessageId (or the error).
    response = TriggerBatchNotifier(
        sns_topic_arn=
            settings.notifications.batch_sns_topic_arn
    ).publish(notification)
    return _sent_status(settings, response)


def _sent_status(settings, response: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "status": "SENT",
        "topic_arn": settings.notifications.batch_sns_topic_arn,
        "message_id": response.get("MessageId"),
    }


def _sns_failed(exc: Exception) -> Dict[str, Any]:
    # Best-effort: a broken SNS topic must not turn a delivered batch into a
    # failed task, but the summary still has to say the event never left.
    logger.exception("Trigger batch completion notification could not be sent")
    return {"status": "FAILED", "error": str(exc)}


def _publish_zero_batch_notification(settings, run_id: str) -> Dict[str, Any]:
    """Announce a genuine empty month to TBB: the same body as a publishing
    run, with zero messages and the current time as the batch window."""
    skip_reason = _batch_config_skip_reason(settings)
    if skip_reason:
        _log_batch_skip(settings, skip_reason, run_id)
        return {"status": "SKIPPED", "reason": skip_reason}

    now = now_timestamp()
    notification = TriggerBatchNotification(
        Trigger_Originating_BU=settings.notifications.trigger_originating_bu,
        No_Of_Messages_Produced=0,
        Trigger_Sub_Type=resolve_trigger(settings.trigger).published_sub_type,
        Topic_Name=settings.kafka.topic,
        Trigger_Batch_Start_Timestamp=now,
        Trigger_Batch_End_Timestamp=now,
        Event_Timestamp=now_timestamp(),
        Correlation_Id=run_id,
    )
    response = TriggerBatchNotifier(
        sns_topic_arn=settings.notifications.batch_sns_topic_arn
    ).publish(notification)
    return _sent_status(settings, response)


def _log_batch_skip(settings, skip_reason: str, run_id: Optional[str]) -> None:
    # Every skip says why: "no SNS in the logs" must be answerable from the logs.
    logger.warning(
        "SNS batch notification skipped: %s",
        skip_reason,
        extra={
            "sns_kind": "batch_complete",
            "sns_skip_reason": skip_reason,
            "sns_topic_arn": settings.notifications.batch_sns_topic_arn,
            "run_id": run_id,
        },
    )


def _batch_config_skip_reason(settings) -> Optional[str]:
    """Why no batch event may be sent at all under this config, or None."""
    if not settings.notifications.batch_notifications_enabled:
        return "batch_notifications_enabled is false"
    if not settings.notifications.batch_sns_topic_arn:
        return "batch_sns_topic_arn is not configured"
    return None


def _batch_notification_skip_reason(settings, result) -> Optional[str]:
    """Why this run must not announce itself to TBB, or None if it should."""
    if result is None:
        return "the run produced no batch result"
    config_reason = _batch_config_skip_reason(settings)
    if config_reason:
        return config_reason
    # SUCCESS already means reconciliation balanced: every row published or
    # quarantined, every publish acknowledged, none failed or left unflushed.
    if result.outcome != "SUCCESS":
        return f"run outcome is {result.outcome}, not SUCCESS"
    # Still possible under SUCCESS when max_quarantine_ratio allows every row to
    # be quarantined.
    if result.counters.acked <= 0:
        return "no messages were acknowledged by Kafka"
    return None


def _notify(settings, classification, config_path: str, task: Dict[str, Optional[str]]) -> None:
    """Best-effort alert. A failing notifier must not mask the original failure."""
    try:
        Notifier(
            sns_topic_arn=settings.notifications.sns_topic_arn,
            application=settings.notifications.application_label,
            environment=settings.app.environment,
        ).notify(
            classification,
            run_context={"config_path": config_path, **{k: v for k, v in task.items() if v}},
        )
    except Exception:
        logger.exception("Failure notification could not be sent")


def _event_from_argv(argv: Optional[List[str]] = None) -> Dict[str, Any]:
    """The run's event from argv: ``trigger 9`` or ``--trigger TRIGGER_9``,
    which wins over IFC_RUN__TRIGGER."""
    parser = argparse.ArgumentParser(prog="ifc-connector-ecs")
    parser.add_argument("trigger_words", nargs="*", metavar="TRIGGER")
    parser.add_argument("--trigger", default=None)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)

    event: Dict[str, Any] = {}
    trigger = args.trigger or " ".join(args.trigger_words)
    if trigger.strip():
        event["trigger"] = trigger
    if args.force:
        event["force"] = True
    return event


def main(argv: Optional[List[str]] = None) -> int:
    try:
        result = ecs_handler(_event_from_argv(argv))
    except Exception:
        logger.exception("ECS entry point failed before the run could start")
        return catalog.CONTAINER_FAILURE.exit_code

    _log_run_summary(result)
    return int(result.get("exit_code", catalog.CONTAINER_FAILURE.exit_code))


def _log_run_summary(result: Dict[str, Any]) -> None:
    """The last line of every run: the answers first, the full summary after."""
    sns = result.get("sns_batch_notification") or {}
    headline = {
        "outcome": result.get("outcome") or ("FAILED" if result.get("exit_code") else None),
        "exit_code": result.get("exit_code"),
        "topic": result.get("topic"),
        "schema_registry_mode": result.get("schema_registry_mode"),
        "schema_id": result.get("schema_id"),
        "acked": result.get("acked"),
        "sns_batch_notification": sns.get("status"),
        "sns_message_id": sns.get("message_id"),
    }
    logger.info(
        "Run summary: %s | %s",
        " ".join(f"{k}={v}" for k, v in headline.items() if v is not None),
        json.dumps(result, default=str),
        extra={"summary": result},
    )


if __name__ == "__main__":
    sys.exit(main())
