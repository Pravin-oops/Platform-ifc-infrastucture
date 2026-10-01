"""ECS entry point for the IFC trigger connector.

The counterpart to ``produce_app``'s ``main_lambda.py``: the same pipeline, but
bootstrapped the way the hosting platform expects. Where the Lambda handler is
handed an event and a context by the runtime, an ECS task is handed environment
variables by the task definition, and it has to survive the two things a Lambda
never sees - a ``SIGTERM`` on every deployment, and a process lifetime longer
than the BAM token it started with.

    ENTRYPOINT ["python", "/app/ifc_trigger_connector/scripts/main_ecs.py"]

Environment (all optional except the config path):

    APP_CONFIG_PATH   connector config YAML, local path or s3://   (required)
    IFC_RUN__MODE     batch | service - overrides the config file
    IFC_RUN__MONTH    YYYY-MM (or AUGUST_2026) - reprocess that month instead of
                      the current one; add IFC_RUN__FORCE=true if it was delivered
    IFC_LOG_LEVEL     overrides app.log_level
    IFC_*             any other setting, e.g. IFC_KAFKA__TOPIC

``ecs_handler()`` returns the run summary as a dict so the same code can be
driven from a test, an ECS RunTask, or a resident ECS service. ``main()`` maps
that summary onto the process exit code, because the stopped-task record is the
only thing left after the container is gone.
"""

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

# Pick up APP_CONFIG_PATH and the IFC_ overlay from <app root>/.env when one is
# present. Variables injected by the ECS task definition always win.
load_dotenv(APP_ROOT / ".env", override=False)

from utility import failure_catalog as catalog
from utility.audit_utility import new_run_id, write_invocation_manifest
from utility.connector_config import load_settings
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

DEFAULT_CONFIG_ENV = "APP_CONFIG_PATH"


def ecs_task_metadata() -> Dict[str, Optional[str]]:
    """Identity of this task, for the run manifest and for alert routing.

    ECS injects the metadata endpoint URI; the ARN suffix is the task id that
    the stopped-task record and the CloudWatch log stream are keyed on.
    """
    return {
        "cluster": os.getenv("ECS_CLUSTER"),
        "task_arn": os.getenv("ECS_TASK_ARN"),
        "container_name": os.getenv("ECS_CONTAINER_NAME"),
        "metadata_uri": os.getenv("ECS_CONTAINER_METADATA_URI_V4"),
        "image_tag": os.getenv("IMAGE_TAG"),
    }


def _trigger_for(settings) -> str:
    """Which trigger this invocation publishes. No default: defaulting would
    silently publish the wrong trigger's month."""
    if not settings.run.trigger:
        raise RuntimeError(
            "No trigger specified: pass it as an argument ('trigger 9'), pass "
            "event['trigger'], set IFC_RUN__TRIGGER, or set run.trigger "
            "(TRIGGER_8 | TRIGGER_9 | TRIGGER_21)"
        )
    return settings.run.trigger


def _load(event: Dict[str, Any]):
    """Resolve the config, set the log level, return (settings, config_path)."""
    config_path = event.get("config_path") or os.getenv(DEFAULT_CONFIG_ENV)
    if not config_path:
        raise RuntimeError(
            f"Missing config path: pass event['config_path'] or set {DEFAULT_CONFIG_ENV}"
        )

    # Configured twice on purpose: once so a config-loading failure is logged in
    # the right format, then again at the level the config asks for.
    configure_logging(os.getenv("IFC_LOG_LEVEL", "INFO"))
    settings = load_settings(config_path)
    # The trigger decides where the data is read from, so it is fixed here -
    # before anything logs or preflights the source path.
    settings.select_trigger(event.get("trigger"), data_path=event.get("data_path"))
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
    """A claimed gate: which trigger this run is for, and where to record it.

    The two travel together because they are only ever both set or both absent -
    keeping them as separate optionals let a caller reach the marker without the
    trigger, which the type checker rightly objected to.
    """

    trigger: str
    marker: RunMarker


def _gate(settings, event: Dict[str, Any]) -> Tuple[Optional[Gate], Optional[str]]:
    """Decide whether to process. Returns (gate, skip_reason).

    ``skip_reason`` is None when the run should proceed; ``gate`` is None when
    gating is off entirely. Runs before preflight, Kafka and the source, so a
    weekend invocation costs only a container start.

    Both skips - a weekend, and a month already delivered - record NOT RAN, so
    the file says this date looked and did nothing rather than staying silent.
    NOT RAN never changes a month's outcome, so a delivered month stays SUCCESS.
    """
    if not settings.gate_active:
        return None, None

    gate = Gate(_trigger_for(settings), RunMarker(settings.run_marker.path))
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
    """Append this invocation's outcome to the run marker file.

    Best-effort: the run has already happened, and losing the marker must not
    turn a delivered month into a failed task. It is logged loudly instead,
    because a missing SUCCESS means the next invocation republishes the month.
    """
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
    """Upstream reconciliation check. Returns a summary when the run stops here.

    ``None`` means proceed. A block is a *reportable* outcome, not a silent
    skip: it carries the catalogue exit code so the ECS stopped-task record
    names the scenario, and it raises the same alert a failure would, because
    nobody is watching the logs on a monthly schedule.

    The exception is a genuine empty month (``NO_DATA_THIS_MONTH``): upstream
    reconciled zero records, which is a delivered month with nothing in it.
    It exits 0 without an alert and announces a zero-message batch to TBB.
    """
    if not settings.recon_active:
        logger.info(
            "Upstream reconciliation gate is not active",
            extra={"enabled": settings.recon.enabled, "trigger": settings.run.trigger},
        )
        return None

    month = month_of(execution_date())
    try:
        decision = recon_gate.evaluate(settings.recon, execution_month=month)
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
            "config_path": config_path,
            "ecs": task,
        }
        if invocation is not None:
            summary["run_id"] = invocation.record(
                "RECON_GATE", summary["outcome"], summary["exit_code"],
                reason=summary["reason"], classification=classification,
                gate={"execution_month": month},
            )
        return summary

    if decision.proceed:
        logger.info(
            "Upstream reconciliation passed",
            extra={"execution_month": month, **decision.to_dict()},
        )
        return None

    if decision.no_data:
        return _no_data_month(settings, decision, month, config_path, task, invocation)

    logger.error(
        "Upstream reconciliation blocked the run: %s",
        decision.reason,
        extra={"execution_month": month, **decision.to_dict()},
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
    """A genuine empty month: nothing to publish, and the month is delivered.

    TBB still gets its batch-completion event - the same body as a publishing
    run, with zero messages - so "no event" never has to be read as "no data".
    """
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
    """Classify, alert and summarise a failure that never reached the runner.

    A startup failure never reaches the runner's own manifest writer, so the
    notification is raised here - the container is about to exit and the
    evidence has to leave with it.
    """
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
    """Run one connector lifecycle and return its summary.

    ``event`` mirrors the Lambda handler's event so an ECS RunTask override, or
    a test, can supply ``config_path`` / ``data_path`` without touching the
    environment. Environment values win only when the event omits them.
    """
    event = event or {}
    settings, config_path = _load(event)
    task = ecs_task_metadata()
    invocation = Invocation(settings)

    if settings.run.month:
        logger.warning(
            "Reprocessing %s: run.month overrides the current month for the source "
            "folder, the business month and the run marker",
            settings.run.month,
            extra={"run_month": settings.run.month, "force": settings.run.force},
        )

    registry = settings.schema_registry
    logger.info(
        "Connector starting on ECS: trigger=%s run_mode=%s topic=%s schema_registry_mode=%s "
        "schema_id=%s kafka_connection=%s environment=%s",
        settings.run.trigger,
        settings.run.mode,
        settings.kafka.topic,
        registry.mode,
        # SECURE resolves the id from the registry at startup; the
        # "Kafka target ready" line reports what it resolved to.
        registry.schema_id if registry.mode == "DEV" else "from registry",
        "BSP" if settings.kafka.bsp_config_path else "direct",
        settings.app.environment,
        extra={
            "config_path": config_path,
            "run_mode": settings.run.mode,
            "topic": settings.kafka.topic,
            "trigger": settings.run.trigger,
            "schema_registry_mode": registry.mode,
            "schema_registry_url": registry.url,
            "configured_schema_id": registry.schema_id,
            # Both: the template says what was configured, the resolved folder
            # says which month this invocation actually went to.
            "source_path": settings.source.path,
            "source_folder": _resolved_source(settings),
            **{k: v for k, v in task.items() if v},
        },
    )

    try:
        gate, skip_reason = _gate(settings, event)
    except Exception as exc:
        # No trigger named, or the run marker file unreadable: still an
        # invocation, so it is recorded before the failure propagates.
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

    # The run gate has just said this invocation should do work; this says
    # whether upstream produced anything to do it with. It runs after the
    # weekend / already-delivered skips deliberately - those are days the run is
    # not meant to happen at all, and checking upstream on them would alert six
    # times a month for nothing. It runs before preflight, Kafka, the source and
    # the marker claim, so a blocked invocation costs only a container start.
    blocked = _recon_gate(settings, config_path, task, invocation)
    if blocked is not None:
        if blocked.get("outcome") == recon_gate.OUTCOME_NO_DATA:
            # A genuine empty month is delivered: SUCCESS with zero records, so
            # the rest of the window stands down instead of announcing it again.
            _record(gate, run_gate.STATUS_SUCCESS, records=0, reason=recon_gate.OUTCOME_NO_DATA)
        else:
            # Upstream had nothing ready, so the connector did not run. Recorded,
            # so the file distinguishes "we looked and upstream was not ready"
            # from "nobody looked".
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
            "mode": settings.run.mode,
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
            # Which month's folder, and which extract inside it. The stopped-task
            # record is all that survives the container, so "which file did this
            # run actually publish" has to be answerable from the summary.
            "source_folder": _resolved_source(settings),
            "source_objects": runner.last_result.source_objects if runner.last_result else [],
            "ecs": task,
        }

        # Only a run that delivered closes the month. Anything else records
        # FAILURE, so the next date in the window retries and the file says
        # what happened rather than staying silent.
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


def _resolved_source(settings) -> Optional[str]:
    """The source location with its date tokens expanded, for the startup log.

    Best-effort: a config with no location resolved yet must not stop the run
    before the real error is raised where it can be classified.
    """
    try:
        return settings.source.resolved_path
    except Exception:  # pragma: no cover - defensive
        return None


def _publish_batch_notification(
    settings,
    runner,
) -> Dict[str, Any]:
    """
    Publish successful Trigger Backbone batch notification.

    TBB starts downstream processing from this event,
    therefore publish only after a fully successful batch.

    Returns what happened (SENT or SKIPPED) for the run summary.
    """

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
            datetime.now(timezone.utc).isoformat(
                timespec="milliseconds"
            ),

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
        Trigger_Sub_Type=resolve_trigger(_trigger_for(settings)).published_sub_type,
        Topic_Name=settings.kafka.topic,
        Trigger_Batch_Start_Timestamp=now,
        Trigger_Batch_End_Timestamp=now,
        Event_Timestamp=datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
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
    if result.outcome != "SUCCESS":
        return f"run outcome is {result.outcome}, not SUCCESS"
    if not result.reconciliation.balanced:
        return (
            f"reconciliation not balanced (expected={result.reconciliation.expected}, "
            f"accounted={result.reconciliation.accounted})"
        )
    if result.counters.acked <= 0:
        return "no messages were acknowledged by Kafka"
    if result.counters.delivery_failed:
        return f"{result.counters.delivery_failed} message(s) failed delivery"
    if result.counters.unflushed:
        return f"{result.counters.unflushed} message(s) were never flushed"
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
    """The scheduler may name the trigger on the command line instead of the
    environment: ``main_ecs.py trigger 9``, ``main_ecs.py "trigger 9"`` or
    ``main_ecs.py --trigger TRIGGER_9``. The argument wins over IFC_RUN__TRIGGER."""
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
