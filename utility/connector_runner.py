"""The connector run loop.

One batch is: stream events from the Trigger BDP, build and validate each
envelope, serialise, size-check, publish, flush, reconcile, write the manifest.

Nothing is de-duplicated here: the consuming team resolves duplicates, so a
re-run republishes the month rather than the connector keeping durable state to
recognise what it already sent.

``batch`` mode does that once and exits - the shape that matches the monthly
cadence under EventBridge Scheduler and ECS RunTask. ``service`` mode repeats it
on an interval for a resident ECS service. The batch body is identical either
way, so the two modes cannot drift apart.

Every exit is deliberate and carries a catalogue exit code, so ECS's
``stoppedReason`` and exit code alone tell RTB which scenario fired.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from ifc_trigger_connector.utility.audit_utility import AuditWriter, RunCounters, build_manifest, new_run_id, reconcile
from ifc_trigger_connector.utility import failure_catalog as catalog
from ifc_trigger_connector.utility.error_classifier import (
    Classification,
    ConnectorError,
    PublishError,
    RecordRejected,
    ZeroRecordsError,
    classify,
)
from ifc_trigger_connector.utility.failure_notifier import Notifier
from ifc_trigger_connector.utility.health_utility import HealthState
from ifc_trigger_connector.utility.kafka_factory import KafkaStack, KafkaStackFactory
from ifc_trigger_connector.utility.observability_utility import Metrics, memory_limit_mb, process_rss_mb, set_log_context
from ifc_trigger_connector.utility.resilience_utility import CircuitBreaker, CircuitOpen, ShutdownSignal
from ifc_trigger_connector.utility.connector_config import ConnectorSettings
from ifc_trigger_connector.utility.trigger_source import ParseFailure, TriggerSource
from ifc_trigger_connector.utility.sequence_allocator import SequenceAllocator
from ifc_trigger_connector.utility.tb_outcome_schema import BuiltRecord, EnvelopeBuilder, TriggerEvent

logger = logging.getLogger(__name__)

#: How often the produce loop reports progress and refreshes health/metrics.
PROGRESS_INTERVAL = 500


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass
class BatchResult:
    counters: RunCounters
    reconciliation: Any
    delivery: Dict[str, Any]
    outcome: str
    exit_code: int
    classification: Optional[Classification] = None
    source_objects: List[str] = field(default_factory=list)
    #: Earliest and latest ``triggerPostingTimestamp`` published by this batch,
    #: and the published sub-type they carried. The Trigger Backbone completion
    #: notification reports the window; none of them is set by a batch that
    #: published nothing.
    batch_start_timestamp: Optional[str] = None
    batch_end_timestamp: Optional[str] = None
    published_trigger_subtype: Optional[str] = None

    @property
    def had_work(self) -> bool:
        return self.counters.records_parsed > 0 or self.counters.parse_failures > 0


class ConnectorRunner:
    def __init__(
        self,
        settings: ConnectorSettings,
        *,
        shutdown: Optional[ShutdownSignal] = None,
        health: Optional[HealthState] = None,
        metrics: Optional[Metrics] = None,
        producer_factory: Any = None,
    ):
        self._settings = settings
        self._shutdown = shutdown or ShutdownSignal()
        self._health = health or HealthState()
        self._metrics = metrics or Metrics(
            dimensions={
                "Environment": settings.app.environment,
                "Application": settings.app.name,
                "Topic": settings.kafka.topic,
            }
        )
        self._producer_factory = producer_factory

        self._run_id = new_run_id()
        self._sequence = SequenceAllocator()
        self._source = TriggerSource(settings.source)
        self._audit = AuditWriter(settings.audit, run_id=self._run_id, environment=settings.app.environment)
        self._notifier = Notifier(
            sns_topic_arn=settings.notifications.sns_topic_arn,
            application=settings.notifications.application_label,
            environment=settings.app.environment,
        )
        self._breaker = CircuitBreaker(
            threshold=settings.resilience.circuit_breaker_threshold,
            reset_seconds=settings.resilience.circuit_breaker_reset_seconds,
        )

        self._stack: Optional[KafkaStack] = None
        self._envelopes: Optional[EnvelopeBuilder] = None
        self._preflight: Optional[Dict[str, Any]] = None
        self._last_result: Optional[BatchResult] = None

        self._batch_start_timestamp: Optional[str] = None
        self._batch_end_timestamp: Optional[str] = None
        self._published_trigger_subtype: Optional[str] = None

        set_log_context(run_id=self._run_id, environment=settings.app.environment)

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def health(self) -> HealthState:
        return self._health

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    @property
    def last_result(self) -> Optional[BatchResult]:
        """The finished batch, for a caller that needs more than the exit code.

        The run-gate needs to know whether anything was actually delivered
        before it closes the execution month; an exit code cannot distinguish
        'published nothing because there was nothing' from 'published 400'.
        """
        return self._last_result

    # -- startup -----------------------------------------------------------

    def start(self) -> None:
        """Preflight, then build the publishing stack. Raises on any blocker."""
        settings = self._settings
        factory = KafkaStackFactory(
            settings,
            metrics=self._metrics,
            shutdown=self._shutdown,
            producer_factory=self._producer_factory,
        )

        if settings.resilience.preflight_enabled:
            from ifc_trigger_connector.utility import kafka_preflight as pf

            pf.check_source(factory.report, path=settings.source.resolved_path, lister=self._source.first_object)
            factory.report.raise_if_failed()

        self._stack = factory.build()
        self._preflight = self._stack.preflight

        self._envelopes = EnvelopeBuilder(
            avro_schema=self._stack.serializer.schema,
            sequence_allocator=self._sequence,
        )

        self._health.mark_startup_complete(
            {"schema_id": self._stack.schema_id, "topic": settings.kafka.topic}
        )
        logger.info(
            "Connector ready",
            extra={"schema_id": self._stack.schema_id, "run_mode": settings.run.mode},
        )

    # -- per-record handling -----------------------------------------------

    def _handle_rejection(
        self,
        *,
        trigger_id: Optional[str],
        rejection: RecordRejected,
        record: Optional[Dict[str, Any]] = None,
        raw: Optional[str] = None,
        counters: RunCounters,
    ) -> None:
        """Quarantine the rejected record to S3.

        The quarantine object is the whole record of a rejection: there is no
        DLQ topic, so this is where a rejected record is recovered from and the
        only place its reason is kept.
        """
        counters.quarantined += 1
        self._metrics.incr("RecordsQuarantined")

        self._audit.quarantine(
            trigger_id=trigger_id,
            reason=str(rejection),
            scenario_key=rejection.scenario.key,
            detail=rejection.detail,
            record=record,
            raw=raw,
        )

    def _publish_record(self, built: BuiltRecord, counters: RunCounters) -> None:
        assert self._stack is not None

        payload = self._stack.serializer(built.record)

        posting_ts = built.record["triggerPostingTimestamp"]
        if (
            self._batch_start_timestamp is None
            or posting_ts < self._batch_start_timestamp
        ):
            self._batch_start_timestamp = posting_ts

        if (
            self._batch_end_timestamp is None
            or posting_ts > self._batch_end_timestamp
        ):
            self._batch_end_timestamp = posting_ts

        self._published_trigger_subtype = (
            built.definition.published_sub_type
        )

        self._stack.size_guard.check(
            payload, trigger_id=built.trigger_id, key=built.kafka_key, record=built.record
        )

        self._audit.write_payload(
            trigger_id=built.trigger_id,
            avro_bytes=payload,
            event_date=built.business_month.replace("-", ""),
        )

        self._stack.publisher.publish(
            key=built.kafka_key,
            value=payload,
            trigger_id=built.trigger_id,
            headers=[
                ("triggerId", built.trigger_id.encode("utf-8")),
                # Header mirrors the envelope field, so a consumer routing on
                # headers and one decoding the record agree.
                ("triggerSubType", built.definition.published_sub_type.encode("utf-8")),
                ("runId", self._run_id.encode("utf-8")),
            ],
        )
        counters.published += 1

    # -- batch -------------------------------------------------------------

    def _classify_outcome(
        self,
        outcome: str,
        exit_code: int,
        classification: Optional[Classification],
        counters: RunCounters,
        reconciliation,
        stream_exhausted: bool,
        source_unreadable: bool = False,
    ):
        """Decide the final outcome of a run that did not fail outright.

        Four ways a technically-successful batch is still not a clean run, in
        priority order: its source could not be read, it found nothing, its
        counts do not reconcile, or it rejected more than the tolerated
        fraction. Returns ``(outcome, exit_code, classification)``.
        """
        settings = self._settings

        if outcome != "SUCCESS":
            return outcome, exit_code, classification

        if source_unreadable and not counters.records_parsed:
            # An unreadable object arrives as a ParseFailure, which on its own
            # would leave the run SUCCESS with one quarantined "record" - so a
            # missing monthly file would exit 0 and look clean. Under the
            # one-file contract that object *is* the batch, so failing to read
            # it fails the run. BDP_READ_FAILURE names missing files among its
            # causes and is a HIGH-severity abort.
            return (
                "SOURCE_UNREADABLE",
                catalog.BDP_READ_FAILURE.exit_code,
                classify(
                    ConnectorError(
                        f"Could not read the trigger source at {settings.source.path}; "
                        "no records were published",
                        catalog.BDP_READ_FAILURE,
                    ),
                    operation="run_batch",
                ),
            )

        if stream_exhausted and not counters.records_parsed and not counters.parse_failures:
            # An empty source on a scheduled run means TED or FRED produced
            # nothing. That is a reportable condition, not a clean run.
            return (
                "ZERO_RECORDS",
                catalog.TED_MISSING_SOURCE_DATA.exit_code,
                classify(
                    ZeroRecordsError(
                        f"No trigger events found under {settings.source.path}",
                        catalog.TED_MISSING_SOURCE_DATA,
                    ),
                    operation="run_batch",
                ),
            )

        if not reconciliation.balanced:
            return (
                "RECONCILIATION_FAILED",
                catalog.RECONCILIATION_FAILURE.exit_code,
                classify(
                    ConnectorError(
                        "; ".join(reconciliation.findings),
                        catalog.RECONCILIATION_FAILURE,
                        context=reconciliation.to_dict(),
                    ),
                    operation="reconcile",
                    topic=settings.kafka.topic,
                ),
            )

        seen = counters.records_parsed + counters.parse_failures
        rejected = counters.quarantined + counters.parse_failures
        ratio = rejected / seen if seen else 0.0
        if ratio > settings.resilience.max_quarantine_ratio:
            return (
                "QUALITY_GATE_FAILED",
                catalog.SCHEMA_VALIDATION_FAILURE.exit_code,
                classify(
                    ConnectorError(
                        f"{ratio:.1%} of records were rejected, over the "
                        f"{settings.resilience.max_quarantine_ratio:.1%} tolerance",
                        catalog.SCHEMA_VALIDATION_FAILURE,
                        context={"quarantine_ratio": round(ratio, 4)},
                    ),
                    operation="quality_gate",
                    topic=settings.kafka.topic,
                ),
            )

        return outcome, exit_code, classification

    def run_batch(self, *, skip_objects: Optional[Set[str]] = None) -> BatchResult:
        assert self._stack is not None and self._envelopes is not None

        settings = self._settings
        counters = RunCounters()
        classification: Optional[Classification] = None
        outcome = "SUCCESS"
        exit_code = catalog.EXIT_OK
        stream_exhausted = False
        source_unreadable = False

        try:
            for index, item in enumerate(self._source.stream(skip_objects=skip_objects), start=1):
                if self._shutdown.is_set:
                    logger.warning("Shutdown signalled; stopping intake and draining")
                    self._health.mark_draining()
                    outcome = "DRAINED"
                    exit_code = catalog.EXIT_WORK_REMAINING
                    break

                self._breaker.raise_if_open()

                if isinstance(item, ParseFailure):
                    counters.parse_failures += 1
                    self._metrics.incr("ParseFailures")
                    if item.scenario_key == catalog.BDP_READ_FAILURE.key:
                        source_unreadable = True
                    self._audit.quarantine(
                        trigger_id=None,
                        reason=item.error,
                        scenario_key=item.scenario_key,
                        detail={"source_object": item.source_object, "index": item.index},
                        raw=item.raw,
                    )
                    continue

                counters.records_parsed += 1
                self._process_event(item, counters)

                if index % PROGRESS_INTERVAL == 0:
                    self._progress(counters)
            else:
                stream_exhausted = True

        except CircuitOpen as exc:
            classification = classify(
                exc.last_error or exc, operation="run_batch", topic=settings.kafka.topic
            )
            outcome = "ABORTED"
            exit_code = classification.exit_code
            logger.error("Circuit breaker abandoned the batch", extra=classification.to_dict())

        except ConnectorError as exc:
            classification = classify(exc, operation="run_batch", topic=settings.kafka.topic)
            outcome = "FAILED"
            exit_code = exc.exit_code

        except Exception as exc:
            classification = classify(exc, operation="run_batch", topic=settings.kafka.topic)
            outcome = "FAILED"
            exit_code = classification.exit_code
            logger.exception("Batch failed", extra=classification.to_dict())

        # Flush regardless of how the loop ended: messages already queued must
        # be given their chance to land before anything is reported.
        counters.unflushed = self._drain()

        stats = self._stack.publisher.stats
        counters.acked = stats.success
        counters.delivery_failed = stats.failure
        counters.objects_read = len(self._source.objects_read)

        reconciliation = reconcile(counters)

        outcome, exit_code, classification = self._classify_outcome(
            outcome,
            exit_code,
            classification,
            counters,
            reconciliation,
            stream_exhausted,
            source_unreadable=source_unreadable,
        )

        return BatchResult(
            counters=counters,
            reconciliation=reconciliation,
            delivery=stats.to_dict(),
            outcome=outcome,
            exit_code=exit_code,
            classification=classification,
            source_objects=self._source.objects_read,
            batch_start_timestamp=self._batch_start_timestamp,
            batch_end_timestamp=self._batch_end_timestamp,
            published_trigger_subtype=self._published_trigger_subtype,
        )

    def _process_event(self, event: TriggerEvent, counters: RunCounters) -> None:
        assert self._envelopes is not None

        try:
            built = self._envelopes.build(event)
        except RecordRejected as rejection:
            self._handle_rejection(
                trigger_id=None,
                rejection=rejection,
                raw=str(event.attributes)[:2000],
                counters=counters,
            )
            return

        try:
            self._publish_record(built, counters)
            self._breaker.record_success()
        except RecordRejected as rejection:
            self._handle_rejection(
                trigger_id=built.trigger_id,
                rejection=rejection,
                record=built.record,
                counters=counters,
            )
        except PublishError as exc:
            self._breaker.record_failure(exc)
            if not exc.scenario.retryable:
                raise
            logger.error(
                "Publish attempt failed",
                extra={"trigger_id": built.trigger_id, "scenario": exc.scenario.key},
            )

    def _drain(self) -> int:
        assert self._stack is not None
        timeout = float(self._settings.kafka.flush_timeout_seconds)

        if self._shutdown.is_set:
            # Never flush for longer than the container has left to live.
            timeout = min(timeout, float(self._settings.run.shutdown_grace_seconds))

        return self._stack.publisher.flush(timeout)

    def _progress(self, counters: RunCounters) -> None:
        assert self._stack is not None

        self._health.heartbeat()
        rss = process_rss_mb()
        limit = memory_limit_mb()

        if rss is not None:
            self._metrics.gauge("MemoryUsedMB", round(rss, 1))
            if limit:
                self._metrics.gauge("MemoryUtilisation", round(rss / limit, 3))

        stats = self._stack.publisher.stats
        self._metrics.gauge("QueueDepth", self._stack.publisher.queue_depth)
        self._metrics.gauge("AckLatencyMsMax", round(stats.latency_ms_max, 1))
        self._metrics.gauge("TokenSecondsRemaining", round(self._stack.token_provider.seconds_remaining))
        self._metrics.emit()

        logger.info("Progress", extra={**counters.to_dict(), "queue_depth": self._stack.publisher.queue_depth})

    # -- run ---------------------------------------------------------------

    def _report(
        self,
        *,
        result: Optional[BatchResult],
        classification: Optional[Classification],
        outcome: str,
        exit_code: int,
        started_at: str,
        duration_seconds: float,
    ) -> None:
        """Write the manifest, alert, and emit metrics for a finished run.

        Everything here is evidence, not control flow: it runs whether the batch
        succeeded, drained or failed, and a failure to record must not change the
        exit code the run already earned.
        """
        counters = result.counters if result else RunCounters()
        reconciliation = result.reconciliation if result else reconcile(counters)

        manifest = build_manifest(
            run_id=self._run_id,
            settings=self._settings,
            counters=counters,
            reconciliation=reconciliation,
            delivery_stats=result.delivery if result else {},
            preflight=self._preflight,
            started_at=started_at,
            finished_at=_utc_now(),
            duration_seconds=duration_seconds,
            outcome=outcome,
            exit_code=exit_code,
            classification=classification,
            source_objects=result.source_objects if result else [],
            quarantine_keys=self._audit.quarantine_keys,
            schema_id=self._stack.schema_id if self._stack else None,
        )

        try:
            self._audit.write_manifest(manifest)
        except Exception:
            logger.exception("Manifest write failed; the run record exists in the log only")

        if classification is not None:
            self._notifier.notify(
                classification,
                run_context={"run_id": self._run_id, "outcome": outcome, **counters.to_dict()},
            )

        self._metrics.incr("RunsCompleted")
        self._metrics.gauge("RunExitCode", exit_code)
        if self._stack is not None:
            self._metrics.gauge(
                "AckLatencyMsMax", round(self._stack.publisher.stats.latency_ms_max, 1)
            )
        self._metrics.emit({"run_id": self._run_id, "outcome": outcome})

        logger.info(
            "Run finished",
            extra={"outcome": outcome, "exit_code": exit_code, **counters.to_dict()},
        )

    def run(self) -> int:
        settings = self._settings
        started_at = _utc_now()
        started = time.perf_counter()
        result: Optional[BatchResult] = None
        classification: Optional[Classification] = None
        exit_code = catalog.EXIT_OK
        outcome = "SUCCESS"

        try:
            self.start()

            if settings.run.mode == "batch":
                result = self.run_batch()
                outcome, exit_code = result.outcome, result.exit_code
            else:
                outcome, exit_code, result = self._run_service()

        except ConnectorError as exc:
            classification = classify(exc, operation="run", topic=settings.kafka.topic)
            outcome, exit_code = "FAILED", exc.exit_code
            logger.error("Run failed", extra=classification.to_dict())

        except Exception as exc:
            classification = classify(exc, operation="run", topic=settings.kafka.topic)
            outcome, exit_code = "FAILED", classification.exit_code
            logger.exception("Run failed with an unhandled error")

        finally:
            self._health.mark_draining()

        self._last_result = result
        self._report(
            result=result,
            classification=classification or (result.classification if result else None),
            outcome=outcome,
            exit_code=exit_code,
            started_at=started_at,
            duration_seconds=time.perf_counter() - started,
        )
        return exit_code

    def _run_service(self) -> Tuple[str, int, Optional[BatchResult]]:
        """Poll the source until shutdown, or until ``max_batches`` is reached."""
        settings = self._settings
        batches = 0
        last: Optional[BatchResult] = None

        while not self._shutdown.is_set:
            batches += 1
            logger.info("Starting batch", extra={"batch": batches})
            self._health.mark_ready()

            last = self.run_batch()

            # A zero-record poll is normal for a resident service; only a
            # scheduled batch run treats it as an upstream failure.
            if last.outcome == "ZERO_RECORDS":
                last.outcome, last.exit_code = "IDLE", catalog.EXIT_OK

            if last.exit_code not in (catalog.EXIT_OK, catalog.EXIT_WORK_REMAINING):
                logger.error(
                    "Batch ended with a blocking failure; stopping the service loop",
                    extra={"outcome": last.outcome, "exit_code": last.exit_code},
                )
                return last.outcome, last.exit_code, last

            if settings.run.max_batches and batches >= settings.run.max_batches:
                logger.info("Reached max_batches", extra={"batches": batches})
                break

            if self._shutdown.sleep(settings.run.poll_interval_seconds):
                break

        if self._shutdown.is_set:
            reason = self._shutdown.reason or "shutdown"
            logger.info("Service loop stopped", extra={"reason": reason})
            return "DRAINED", catalog.EXIT_OK, last

        return (last.outcome if last else "SUCCESS"), catalog.EXIT_OK, last