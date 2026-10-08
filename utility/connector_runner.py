"""The connector run loop."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from utility.audit_utility import AuditWriter, RunCounters, build_manifest, new_run_id, reconcile
from utility import failure_catalog as catalog
from utility.error_classifier import (
    Classification,
    ConnectorError,
    PublishError,
    RecordRejected,
    ZeroRecordsError,
    classify,
)
from utility.failure_notifier import Notifier
from utility.health_utility import HealthState
from utility.kafka_factory import KafkaStack, KafkaStackFactory
from utility.observability_utility import Metrics, memory_limit_mb, process_rss_mb, set_log_context
from utility.resilience_utility import BackoffPolicy, CircuitBreaker, CircuitOpen, ShutdownSignal, retry
from utility.connector_config import ConnectorSettings
from utility.connector_utility import SourceAccessError
from utility.trigger_source import make_source
from utility.run_gate import execution_date
from utility.sequence_allocator import SequenceAllocator
from utility.tb_outcome_schema import BuiltRecord, EnvelopeBuilder, TriggerEvent, previous_month

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
    #: The table, business date and Athena query execution id this run read.
    source: Dict[str, Any] = field(default_factory=dict)
    #: Earliest and latest triggerPostingTimestamp published, and their sub-type.
    batch_start_timestamp: Optional[str] = None
    batch_end_timestamp: Optional[str] = None
    published_trigger_subtype: Optional[str] = None


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
        # Resolved here, before any network call, so an environment with no
        # code configured stops the run instead of publishing under another's.
        self._originating_system = settings.originating_system
        self._sequence = SequenceAllocator()
        # Fixed once, so the rows queried and the business month stamped on them
        # cannot disagree even if the run crosses midnight at a month end.
        self._business_month = previous_month(execution_date())
        self._source = make_source(
            settings.source, trigger=settings.trigger, business_month=self._business_month
        )
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
        self._backoff = BackoffPolicy(
            base_seconds=settings.resilience.backoff_base_seconds,
            max_seconds=settings.resilience.backoff_max_seconds,
        )

        self._stack: Optional[KafkaStack] = None
        self._envelopes: Optional[EnvelopeBuilder] = None
        self._preflight: Optional[Dict[str, Any]] = None
        self._last_result: Optional[BatchResult] = None

        self._batch_start_timestamp: Optional[str] = None
        self._batch_end_timestamp: Optional[str] = None
        self._published_trigger_subtype: Optional[str] = None
        #: (column, check) -> count and first reason, for this batch's rejections.
        self._rejections: Dict[Tuple[str, str], Dict[str, Any]] = {}

        set_log_context(run_id=self._run_id, environment=settings.app.environment)

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def schema_id(self) -> Optional[int]:
        """The id records were framed with; None if the run stopped before startup finished."""
        return self._stack.schema_id if self._stack else None

    @property
    def health(self) -> HealthState:
        return self._health

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    @property
    def last_result(self) -> Optional[BatchResult]:
        """The finished batch, for a caller that needs more than the exit code."""
        return self._last_result

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
            from utility import kafka_preflight as pf

            pf.check_source(factory.report, table=settings.source.resolved_table, probe=self._source.check_access)
            factory.report.raise_if_failed()

        self._stack = factory.build()
        self._preflight = self._stack.preflight
        factory.report.log_summary()

        self._envelopes = EnvelopeBuilder(
            avro_schema=self._stack.serializer.schema,
            originating_system=self._originating_system,
            declare_encryption_policies=settings.declares_encryption_policies,
            sequence_allocator=self._sequence,
            business_month=self._business_month,
        )
        logger.info(
            "Envelope identity: triggerOriginatingSystem=idSystem=%s, field encryption "
            "policies %s (environment %s)",
            self._originating_system,
            "declared" if settings.declares_encryption_policies else "empty",
            settings.app.environment,
        )

        self._health.update(schema_id=self._stack.schema_id, topic=settings.kafka.topic)
        self._log_kafka_target()

    def _log_kafka_target(self) -> None:
        """One line that says where this run publishes and how records are framed."""
        assert self._stack is not None
        settings = self._settings
        context = self._stack.schema_context
        mode = settings.schema_registry.mode

        if not context.get("subject"):
            schema_source = "pinned in config (not checked against the registry)"
        else:
            schema_source = (
                f"registry subject {context.get('subject')} version {context.get('schema_version')}"
            )
        connection = "BSP" if settings.kafka.bsp_config_path else "direct (no BSP, local only)"
        header = self._stack.serializer.header.hex(" ")

        logger.info(
            "Kafka target ready: topic=%s schema_registry_mode=%s schema_id=%s "
            "schema_source=%s wire_format_header=%s connection=%s",
            settings.kafka.topic,
            mode,
            self._stack.schema_id,
            schema_source,
            header,
            connection,
            extra={
                "topic": settings.kafka.topic,
                "schema_registry_mode": mode,
                "schema_id": self._stack.schema_id,
                "schema_source": schema_source,
                "wire_format_header": header,
                "kafka_connection": connection,
            },
        )

    def _handle_rejection(
        self,
        *,
        trigger_id: Optional[str],
        rejection: RecordRejected,
        row: Optional[int] = None,
        record: Optional[Dict[str, Any]] = None,
        raw: Optional[str] = None,
        counters: RunCounters,
    ) -> None:
        """Quarantine the rejected record to S3."""
        counters.quarantined += 1
        self._metrics.incr("RecordsQuarantined")
        self._note_rejection(trigger_id, rejection, row)

        self._audit.quarantine(
            trigger_id=trigger_id,
            reason=str(rejection),
            scenario_key=rejection.scenario.key,
            detail=rejection.detail,
            record=record,
            raw=raw,
        )

    def _note_rejection(self, trigger_id: Optional[str], rejection: RecordRejected, row: Optional[int]) -> None:
        """Count the rejection by column and check; the first of each kind is logged at WARNING."""
        detail = rejection.detail
        field_name, source_key = detail.get("field_name"), detail.get("source_key")
        column = f"{field_name} ({source_key})" if field_name and source_key else field_name or "-"
        check = detail.get("check") or rejection.scenario.key

        entry = self._rejections.get((column, check))
        first = entry is None
        if first:
            entry = self._rejections[(column, check)] = {
                "column": column, "check": check, "count": 0, "example": str(rejection),
            }
        entry["count"] += 1

        logger.log(
            logging.WARNING if first else logging.DEBUG,
            "Record rejected: column=%s check=%s row=%s trigger_id=%s reason=%s%s",
            column,
            check,
            row,
            trigger_id,
            rejection,
            "; further rows with this problem are counted in the quarantine summary" if first else "",
            extra={"column": column, "check": check, "scenario": rejection.scenario.key},
        )

    def _rejection_summary(self) -> List[Dict[str, Any]]:
        return sorted(self._rejections.values(), key=lambda entry: -entry["count"])

    @staticmethod
    def _describe(rejections: List[Dict[str, Any]]) -> str:
        return "; ".join(f"{r['column']} {r['check']} x{r['count']}" for r in rejections)

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

        self._publish_with_retry(built, payload)
        counters.published += 1

    def _publish_with_retry(self, built: BuiltRecord, payload: bytes) -> None:
        """Publish one record, retrying a retryable failure with backoff."""
        assert self._stack is not None
        publisher = self._stack.publisher

        def worth_retrying(exc: BaseException) -> bool:
            if not isinstance(exc, PublishError):
                return False
            self._breaker.record_failure(exc)
            return exc.scenario.retryable and not self._breaker.is_open

        retry(
            # No Kafka headers: the Trigger Backbone reads everything from the
            # envelope, and its reference records carry an empty header list.
            lambda: publisher.publish(key=built.kafka_key, value=payload, trigger_id=built.trigger_id),
            attempts=self._settings.resilience.max_publish_attempts,
            policy=self._backoff,
            retry_on=worth_retrying,
            shutdown=self._shutdown,
            description=f"publish of {built.trigger_id}",
        )

    def _classify_outcome(
        self,
        outcome: str,
        exit_code: int,
        classification: Optional[Classification],
        counters: RunCounters,
        reconciliation,
        stream_exhausted: bool,
    ):
        """Decide the final outcome of a run that did not fail outright."""
        settings = self._settings

        if outcome != "SUCCESS":
            return outcome, exit_code, classification

        if stream_exhausted and not counters.records_parsed:
            # No rows for the month on a scheduled run means upstream produced
            # nothing. That is a reportable condition, not a clean run.
            return (
                "ZERO_RECORDS",
                catalog.TED_MISSING_SOURCE_DATA.exit_code,
                classify(
                    ZeroRecordsError(
                        f"No trigger events found in {settings.source.table} for this month",
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

        seen = counters.records_parsed
        ratio = counters.quarantined / seen if seen else 0.0
        if ratio > settings.resilience.max_quarantine_ratio:
            return (
                "QUALITY_GATE_FAILED",
                catalog.SCHEMA_VALIDATION_FAILURE.exit_code,
                classify(
                    ConnectorError(
                        f"{ratio:.1%} of records were rejected, over the "
                        f"{settings.resilience.max_quarantine_ratio:.1%} tolerance: "
                        f"{self._describe(self._rejection_summary())}",
                        catalog.SCHEMA_VALIDATION_FAILURE,
                        context={"quarantine_ratio": round(ratio, 4), "rejections": self._rejection_summary()},
                    ),
                    operation="quality_gate",
                    topic=settings.kafka.topic,
                ),
            )

        return outcome, exit_code, classification

    def run_batch(self) -> BatchResult:
        assert self._stack is not None and self._envelopes is not None

        settings = self._settings
        counters = RunCounters()
        self._rejections = {}
        classification: Optional[Classification] = None
        outcome = "SUCCESS"
        exit_code = catalog.EXIT_OK
        stream_exhausted = False

        try:
            for index, item in enumerate(self._source.stream(), start=1):
                if self._shutdown.is_set:
                    logger.warning("Shutdown signalled; stopping intake and draining")
                    self._health.mark_draining()
                    outcome = "DRAINED"
                    exit_code = catalog.EXIT_WORK_REMAINING
                    break

                self._breaker.raise_if_open()

                counters.records_parsed += 1
                self._process_event(item, counters)

                if index % PROGRESS_INTERVAL == 0:
                    self._progress(counters)
            else:
                stream_exhausted = True

        except SourceAccessError as exc:
            # The Athena query could not be run: nothing was read, so nothing
            # can be published. A HIGH-severity Trigger BDP read failure.
            classification = classify(exc, operation="run_batch", topic=settings.kafka.topic)
            outcome = "SOURCE_UNREADABLE"
            exit_code = classification.exit_code
            logger.error("Could not read the trigger table", extra=classification.to_dict())

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

        reconciliation = reconcile(counters)

        logger.info(
            "Kafka publish summary: topic=%s schema_id=%s published=%d acked=%d "
            "delivery_failed=%d unflushed=%d quarantined=%d",
            settings.kafka.topic,
            self._stack.schema_id,
            counters.published,
            counters.acked,
            counters.delivery_failed,
            counters.unflushed,
            counters.quarantined,
            extra={"topic": settings.kafka.topic, "schema_id": self._stack.schema_id, **counters.to_dict()},
        )
        if counters.quarantined:
            rejections = self._rejection_summary()
            logger.warning(
                "Quarantine summary: %d of %d records rejected: %s",
                counters.quarantined,
                counters.records_parsed,
                self._describe(rejections),
                extra={"rejections": rejections},
            )

        outcome, exit_code, classification = self._classify_outcome(
            outcome,
            exit_code,
            classification,
            counters,
            reconciliation,
            stream_exhausted,
        )

        return BatchResult(
            counters=counters,
            reconciliation=reconciliation,
            delivery=stats.to_dict(),
            outcome=outcome,
            exit_code=exit_code,
            classification=classification,
            source=self._source.describe(),
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
                row=event.source_index + 1,
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
                row=event.source_index + 1,
                record=built.record,
                counters=counters,
            )
        except PublishError as exc:
            # The breaker has already counted every failed attempt.
            if not exc.scenario.retryable:
                raise
            logger.error(
                "Record not published after retrying; the reconciliation will fail the run",
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
        """Write the manifest, alert, and emit metrics for a finished run."""
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
            source=result.source if result else self._source.describe(),
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
            "Run finished: outcome=%s exit_code=%s acked=%d quarantined=%d%s",
            outcome,
            exit_code,
            counters.acked,
            counters.quarantined,
            f" reason={classification.raw_error}" if classification is not None else "",
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
            result = self.run_batch()
            outcome, exit_code = result.outcome, result.exit_code

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
