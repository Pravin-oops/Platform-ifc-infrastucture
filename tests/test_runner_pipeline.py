"""End-to-end batch behaviour, with Kafka and the Schema Registry faked out.

These are the tests that actually exercise the ECS-specific behaviour: draining
on SIGTERM, retrying a failed publish, quarantining a poison record without
losing the batch, and refusing to report success when the numbers do not add up.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

import pytest

from utility import failure_catalog as catalog
from utility.connector_utility import load_schema_document
from utility.kafka_factory import KafkaStack
from utility.kafka_publisher import Publisher
from utility.kafka_serializers import AvroSerializer, SizeGuard
from utility.observability_utility import Metrics
from utility.resilience_utility import ShutdownSignal
from utility.connector_runner import ConnectorRunner
from utility.connector_config import ConnectorSettings

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeMessage:
    def __init__(self, topic: str, key: bytes, partition: int, offset: int):
        self._topic, self._key, self._partition, self._offset = topic, key, partition, offset

    def topic(self):
        return self._topic

    def key(self):
        return self._key

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


#: The trigger ID, and so the message key, of the first record of a June 2026
#: Trigger 8 batch.
FIRST_TRIGGER_ID = "SNSVC0084378_KYCRefresh_NewHRCRelationship_2026-06-30T23:59:59.999999999Z_1"


class FakeProducer:
    """Delivers synchronously; ``fail_keys`` forces a delivery error."""

    def __init__(self, *, fail_keys: Optional[set] = None, queue_limit: Optional[int] = None):
        self.produced: List[Dict[str, Any]] = []
        self._fail_keys = fail_keys or set()
        self._queue_limit = queue_limit
        self._offset = 0
        self._pending: List[Any] = []

    def produce(
        self,
        *,
        topic,
        key,
        value,
        headers=None,
        on_delivery: Optional[Callable[[Any, Any], None]] = None,
    ):
        if self._queue_limit is not None and len(self._pending) >= self._queue_limit:
            raise BufferError("queue full")

        self.produced.append({"topic": topic, "key": key, "value": value, "headers": headers})
        decoded = key.decode("utf-8")

        # librdkafka allows produce() without a delivery callback, so the
        # parameter is genuinely optional; bind a no-op rather than calling
        # through an Optional.
        callback: Callable[[Any, Any], None] = on_delivery or (lambda err, msg: None)

        if decoded in self._fail_keys:
            from tests.test_failure_handling import FakeKafkaError

            self._pending.append(lambda: callback(FakeKafkaError("_ALL_BROKERS_DOWN"), None))
        else:
            self._offset += 1
            message = FakeMessage(topic, key, 0, self._offset)
            self._pending.append(lambda: callback(None, message))

    def poll(self, timeout=0):
        # A non-blocking poll(0) serves callbacks that are already due. When a
        # queue limit is set, deliveries are held until a blocking poll, which
        # is what makes the queue fill and BufferError fire - the behaviour the
        # back-pressure path exists for.
        if self._queue_limit is not None and not timeout:
            return 0

        pending, self._pending = self._pending, []
        for callback in pending:
            callback()
        return len(pending)

    def flush(self, timeout=None):
        self.poll(1)
        return 0

    def __len__(self):
        return len(self._pending)


def build_stack(producer: FakeProducer, settings: ConnectorSettings, metrics: Metrics) -> KafkaStack:
    schema = load_schema_document("utility/schema.json")

    class Tokens:
        seconds_remaining = 3600.0

        def get(self, force_refresh: bool = False):
            return "fake.token.value"

    return KafkaStack(
        publisher=Publisher(
            producer,
            topic=settings.kafka.topic,
            metrics=metrics,
        ),
        serializer=AvroSerializer(schema, 101, name="trigger"),
        size_guard=SizeGuard(settings.kafka.max_message_bytes),
        token_provider=Tokens(),
        schema_id=101,
        preflight={"passed": True, "checks": []},
        producer=producer,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

#: One row of the trigger 8 table: the attribute columns and nothing else.
VALID_ROW = {
    "date_of_request": "2026-06-10T02:15:04.221Z",
    "counterparty_full_legal_entity_name": "AbCdEfGh12345",
    "counterparty_csid_sds": 9912345678,
    "customer_segment": "Corporate",
    "client_relationship_owner_brid": "B0412775",
    "client_relationship_owner_name": "XyZwVu67890",
    "client_relationship_owner_business_unit": "UK Corporate",
    "client_relationship_owner_location": "UK",
    "region": "EMEA",
    "business_date": "2026-06-30",
}


def row(**overrides) -> Dict[str, Any]:
    return {**VALID_ROW, **overrides}


class FakeAthena:
    """Answers the calls AthenaTriggerSource makes, serving ``rows``.

    Columns are the union of the rows' keys; ints are typed bigint and
    everything else varchar, and results are paged 1000 rows at a time, the way
    GetQueryResults pages them.
    """

    def __init__(self, rows: List[Dict[str, Any]], *, state: str = "SUCCEEDED"):
        self.rows = rows
        self.state = state
        self.queries: List[Dict[str, Any]] = []

    def start_query_execution(self, **request):
        self.queries.append(request)
        return {"QueryExecutionId": "q-1"}

    def get_query_execution(self, QueryExecutionId):
        status = {"State": self.state, "StateChangeReason": "TABLE_NOT_FOUND"}
        return {"QueryExecution": {"Status": status}}

    def get_table_metadata(self, **_):
        return {}

    def get_paginator(self, _name):
        names: List[str] = []
        for r in self.rows:
            names += [k for k in r if k not in names]
        types = {
            n: "bigint" if any(isinstance(r.get(n), int) for r in self.rows) else "varchar"
            for n in names
        }

        def cell(value):
            return {} if value is None else {"VarCharValue": str(value)}

        data = [{"Data": [cell(r.get(n)) for n in names]} for r in self.rows]
        header = {"Data": [{"VarCharValue": n} for n in names]}
        chunks = [data[i:i + 1000] for i in range(0, len(data), 1000)] or [[]]
        meta = {"ColumnInfo": [{"Name": n, "Type": types[n]} for n in names]}
        pages = [
            {"ResultSet": {"Rows": ([header] if i == 0 else []) + chunk, "ResultSetMetadata": meta}}
            for i, chunk in enumerate(chunks)
        ]

        class Paginator:
            def paginate(self, **_):
                return iter(pages)

        return Paginator()


def make_settings(**overrides) -> ConnectorSettings:
    document = {
        "app": {"environment": "TEST", "log_level": "WARNING"},
        "run": {"shutdown_grace_seconds": 5, "trigger": "TRIGGER_8"},
        "source": {"trigger_tables": {"TRIGGER_8": "ifc_trigger_db.trigger_8"}},
        "kafka": {
            "topic": "test_ifc_topic",
            "bsp_config_path": "config/bsp_local_config.yaml",
        },
        "schema_registry": {"mode": "DEV"},
        "audit": {"bucket": None},
        "resilience": {"preflight_enabled": False, "max_quarantine_ratio": 1.0},
        "health": {"enabled": False},
        # TEST is not a real environment; it publishes under the SIT code.
        "envelope": {"originating_systems": {"TEST": "SNSVC0084378"}},
    }
    for section, values in overrides.items():
        document.setdefault(section, {}).update(values)
    settings = ConnectorSettings.model_validate(document)
    settings.select_trigger(None)
    return settings


def build_runner(rows, *, producer=None, athena=None, **overrides):
    """A runner reading ``rows`` through the real Athena source, Kafka faked."""
    from utility.tb_outcome_schema import EnvelopeBuilder
    from utility.trigger_source import AthenaTriggerSource

    settings = make_settings(**overrides)
    metrics = Metrics()
    # Not ``or``: a FakeProducer with an empty queue has len() 0 and is falsy.
    if producer is None:
        producer = FakeProducer()

    runner = ConnectorRunner(settings, metrics=metrics, shutdown=ShutdownSignal())
    runner._source = AthenaTriggerSource(
        settings.source,
        trigger=settings.run.trigger,
        business_month="2026-06",
        client=athena or FakeAthena(rows),
        sleep=lambda _s: None,
    )
    runner._stack = build_stack(producer, settings, metrics)
    runner._envelopes = EnvelopeBuilder(
        avro_schema=runner._stack.serializer.schema,
        originating_system=runner._originating_system,
        declare_encryption_policies=runner._settings.declares_encryption_policies,
        sequence_allocator=runner._sequence,
        business_month="2026-06",
    )
    return runner, producer, metrics


@pytest.fixture
def runner_factory():
    def make(rows, *, fail_keys=None, **overrides):
        runner, producer, _ = build_runner(
            rows, producer=FakeProducer(fail_keys=fail_keys), **overrides
        )
        return runner, producer

    return make


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_a_valid_batch_publishes_and_reconciles(self, runner_factory):
        runner, producer = runner_factory([VALID_ROW])
        result = runner.run_batch()

        assert result.outcome == "SUCCESS"
        assert result.exit_code == catalog.EXIT_OK
        assert result.counters.published == 1
        assert result.counters.acked == 1
        assert result.reconciliation.balanced
        assert producer.produced[0]["topic"] == "test_ifc_topic"

    def test_the_record_carries_no_headers(self, runner_factory):
        runner, producer = runner_factory([VALID_ROW])
        runner.run_batch()

        assert not producer.produced[0]["headers"]

    def test_the_wire_format_is_magic_byte_plus_schema_id(self, runner_factory):
        runner, producer = runner_factory([VALID_ROW])
        runner.run_batch()

        value = producer.produced[0]["value"]
        assert value[0] == 0
        assert int.from_bytes(value[1:5], "big") == 101


class TestRunLogs:
    """The lines an operator reads in the ECS console to answer 'did it publish?'."""

    def messages(self, caplog, prefix):
        return [r.getMessage() for r in caplog.records if r.getMessage().startswith(prefix)]

    def test_each_acknowledged_message_is_logged_as_published_at_debug(self, runner_factory, caplog):
        """Per message, so DEBUG only: at INFO the publish summary carries the totals."""
        caplog.set_level("DEBUG")
        runner, _ = runner_factory([VALID_ROW])
        runner.run_batch()

        [record] = [r for r in caplog.records if r.getMessage().startswith("Message published:")]
        assert record.levelname == "DEBUG"
        published = record.getMessage()
        assert "topic=test_ifc_topic" in published
        assert f"trigger_id={FIRST_TRIGGER_ID}" in published
        assert "offset=1" in published

    def test_a_failed_delivery_is_logged_as_not_published(self, runner_factory, caplog):
        caplog.set_level("INFO")
        runner, _ = runner_factory([VALID_ROW], fail_keys={FIRST_TRIGGER_ID})
        runner.run_batch()

        assert self.messages(caplog, "Message published:") == []
        [failed] = self.messages(caplog, "Message NOT published:")
        assert f"trigger_id={FIRST_TRIGGER_ID}" in failed

    def test_the_batch_ends_with_a_publish_summary(self, runner_factory, caplog):
        caplog.set_level("INFO")
        runner, _ = runner_factory([VALID_ROW])
        runner.run_batch()

        [summary] = self.messages(caplog, "Kafka publish summary:")
        assert "topic=test_ifc_topic schema_id=101 published=1 acked=1 delivery_failed=0" in summary


class TestQuarantine:
    def test_a_poison_record_is_quarantined_and_the_batch_continues(self, runner_factory):
        poison = {"region": "EMEA"}  # no CSID, so no idValue
        runner, producer = runner_factory([poison, VALID_ROW])

        result = runner.run_batch()

        assert result.counters.quarantined == 1
        assert result.counters.published == 1
        assert result.reconciliation.balanced

    def test_exceeding_the_quarantine_tolerance_fails_the_run(self, runner_factory):
        poison = {"region": "EMEA"}
        runner, _ = runner_factory(
            [poison, VALID_ROW], resilience={"max_quarantine_ratio": 0.1}
        )

        result = runner.run_batch()

        assert result.outcome == "QUALITY_GATE_FAILED"
        assert result.exit_code == catalog.SCHEMA_VALIDATION_FAILURE.exit_code


class TestRejectionReporting:
    """At INFO a rejected row names its column and check, once per kind, plus a summary."""

    ROWS = [
        row(date_of_request="10/06/2026"),
        row(date_of_request="not a date"),
        row(region=None),
        VALID_ROW,
    ]

    def warnings(self, caplog, prefix):
        return [
            r for r in caplog.records
            if r.levelname == "WARNING" and r.getMessage().startswith(prefix)
        ]

    def test_the_first_row_of_each_problem_is_logged_with_its_column(self, runner_factory, caplog):
        caplog.set_level("INFO")
        runner, _ = runner_factory(self.ROWS)
        runner.run_batch()

        lines = [r.getMessage() for r in self.warnings(caplog, "Record rejected:")]
        assert len(lines) == 2
        assert "column=Date of Request (date_of_request) check=not a valid DATE" in lines[0]
        assert "row=1 " in lines[0] and "'10/06/2026'" in lines[0]
        assert "column=Region (region) check=missing" in lines[1]

    def test_the_summary_counts_each_problem(self, runner_factory, caplog):
        caplog.set_level("INFO")
        runner, _ = runner_factory(self.ROWS)
        runner.run_batch()

        [summary] = self.warnings(caplog, "Quarantine summary:")
        assert summary.getMessage() == (
            "Quarantine summary: 3 of 4 records rejected: "
            "Date of Request (date_of_request) not a valid DATE x2; Region (region) missing x1"
        )
        assert summary.rejections[0]["count"] == 2

    def test_the_quality_gate_failure_names_the_columns(self, runner_factory):
        runner, _ = runner_factory(self.ROWS, resilience={"max_quarantine_ratio": 0.05})
        result = runner.run_batch()

        assert result.outcome == "QUALITY_GATE_FAILED"
        assert result.classification.raw_error.endswith(
            "tolerance: Date of Request (date_of_request) not a valid DATE x2; Region (region) missing x1"
        )
        assert result.classification.context["rejections"][1]["column"] == "Region (region)"

    def test_the_run_finished_line_carries_the_reason(self, runner_factory, caplog):
        caplog.set_level("INFO")
        runner, _ = runner_factory(self.ROWS, resilience={"max_quarantine_ratio": 0.05})
        result = runner.run_batch()
        runner._report(
            result=result, classification=result.classification,
            outcome=result.outcome, exit_code=result.exit_code, started_at="t", duration_seconds=0.0,
        )

        [finished] = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Run finished:")]
        assert "reason=75.0% of records were rejected" in finished
        assert "Region (region) missing x1" in finished

    def test_a_clean_batch_logs_no_summary(self, runner_factory, caplog):
        caplog.set_level("INFO")
        runner, _ = runner_factory([VALID_ROW])
        runner.run_batch()

        assert self.warnings(caplog, "Quarantine summary:") == []


class TestSequenceNumbers:
    """No durable state: the consuming team resolves duplicates, so a re-run
    republishes rather than the connector remembering what it sent."""

    def test_a_re_run_republishes_rather_than_suppressing(self, runner_factory):
        runner, producer = runner_factory([VALID_ROW])
        runner.run_batch()
        assert len(producer.produced) == 1

        runner2, producer2 = runner_factory([VALID_ROW])
        result = runner2.run_batch()

        assert result.counters.published == 1
        assert len(producer2.produced) == 1
        assert result.reconciliation.balanced

    def test_each_customer_starts_at_one(self, runner_factory):
        second = row(counterparty_csid_sds=9912345679)
        runner, producer = runner_factory([VALID_ROW, second])
        result = runner.run_batch()

        assert result.counters.acked == 2
        assert runner._sequence.customers == 2
        assert runner._sequence.occurrences("9912345678") == 1
        assert runner._sequence.occurrences("9912345679") == 1

    def test_a_repeated_customer_counts_up(self, runner_factory):
        """Two events from one source for one customer: 1, then 2."""
        runner, producer = runner_factory([VALID_ROW, row()])
        result = runner.run_batch()

        assert result.counters.acked == 2
        assert runner._sequence.customers == 1
        assert runner._sequence.occurrences("9912345678") == 2


class TestFailureOutcomes:
    def test_a_delivery_failure_fails_reconciliation(self, runner_factory):
        runner, _ = runner_factory([VALID_ROW], fail_keys={FIRST_TRIGGER_ID})
        result = runner.run_batch()

        assert not result.reconciliation.balanced
        assert result.outcome == "RECONCILIATION_FAILED"
        assert result.exit_code == catalog.RECONCILIATION_FAILURE.exit_code

    def test_the_dominant_failure_scenario_is_reported(self, runner_factory):
        runner, _ = runner_factory([VALID_ROW], fail_keys={FIRST_TRIGGER_ID})
        result = runner.run_batch()
        assert result.delivery["by_scenario"] == {"BROKER_UNAVAILABLE": 1}

    def test_an_empty_month_is_reported_as_zero_records_not_success(self, runner_factory):
        runner, _ = runner_factory([])

        result = runner.run_batch()

        assert result.outcome == "ZERO_RECORDS"
        assert result.exit_code == catalog.TED_MISSING_SOURCE_DATA.exit_code


class TestPublishRetry:
    """A retryable publish failure is retried; one that persists fails the
    reconciliation rather than being quarantined."""

    #: Fast backoff so the retries do not slow the suite.
    FAST = {"backoff_base_seconds": 0.001, "backoff_max_seconds": 0.001}

    @staticmethod
    def failing_publisher(runner, failures: int):
        """Make the next ``failures`` publishes raise a retryable PublishError."""
        from utility.error_classifier import PublishError

        publisher = runner._stack.publisher
        original = publisher.publish
        calls = {"attempts": 0}

        def publish(**kwargs):
            calls["attempts"] += 1
            if calls["attempts"] <= failures:
                raise PublishError("queue full", catalog.HIGH_PUBLISH_LATENCY)
            return original(**kwargs)

        publisher.publish = publish
        return calls

    def test_a_transient_failure_is_retried_and_the_record_published(self, runner_factory):
        runner, producer = runner_factory(
            [VALID_ROW], resilience={**self.FAST, "max_publish_attempts": 3}
        )
        calls = self.failing_publisher(runner, failures=1)

        result = runner.run_batch()

        assert calls["attempts"] == 2
        assert result.outcome == "SUCCESS"
        assert result.counters.published == 1
        assert result.counters.acked == 1
        assert len(producer.produced) == 1

    def test_a_persistent_failure_fails_the_reconciliation_without_quarantine(self, runner_factory):
        runner, producer = runner_factory(
            [VALID_ROW], resilience={**self.FAST, "max_publish_attempts": 3}
        )
        calls = self.failing_publisher(runner, failures=99)

        result = runner.run_batch()

        assert calls["attempts"] == 3
        assert result.counters.published == 0
        assert result.counters.quarantined == 0
        assert result.outcome == "RECONCILIATION_FAILED"
        assert producer.produced == []

    def test_every_failed_attempt_counts_towards_the_circuit_breaker(self, runner_factory):
        """A total outage must not retry each record in turn: the breaker opens
        on attempts, so the second record is never tried."""
        second = row(counterparty_csid_sds=9912345679)
        runner, _ = runner_factory(
            [VALID_ROW, second],
            resilience={**self.FAST, "max_publish_attempts": 5, "circuit_breaker_threshold": 2},
        )
        calls = self.failing_publisher(runner, failures=99)

        result = runner.run_batch()

        assert calls["attempts"] == 2
        assert result.outcome == "ABORTED"
        assert result.counters.published == 0


class TestShutdown:
    def test_a_signal_stops_intake_and_drains_what_was_queued(self, runner_factory):
        rows = [row(counterparty_csid_sds=9900000000 + i) for i in range(5)]
        runner, producer = runner_factory(rows)
        runner._shutdown.set("test")

        result = runner.run_batch()

        assert result.outcome == "DRAINED"
        assert result.exit_code == catalog.EXIT_WORK_REMAINING
        assert producer.produced == []

    def test_health_reports_draining_after_a_signal(self, runner_factory):
        runner, _ = runner_factory([VALID_ROW])
        runner._shutdown.set("test")
        runner.run_batch()
        assert runner.health.snapshot()["draining"] is True


class TestBackpressure:
    def test_a_full_local_queue_is_waited_out_rather_than_grown(self):
        rows = [row(counterparty_csid_sds=9900000000 + i) for i in range(6)]
        # A queue that only holds two messages forces BufferError repeatedly.
        runner, _, metrics = build_runner(rows, producer=FakeProducer(queue_limit=2))

        result = runner.run_batch()

        assert result.counters.published == 6
        assert metrics.get("BackpressureWaits") > 0

class TestBatchCompletionWindow:
    """What the Trigger Backbone completion notification reports on."""

    def test_a_published_batch_reports_its_posting_window_and_sub_type(self, runner_factory):
        second = row(counterparty_csid_sds=9912345679)
        runner, _ = runner_factory([VALID_ROW, second])
        result = runner.run_batch()

        assert result.counters.acked == 2
        assert result.published_trigger_subtype == "NewHRCRelationship"
        # The window is the posting timestamps - when the records were posted -
        # not the business month every record in the batch shares.
        assert result.batch_start_timestamp is not None
        assert result.batch_end_timestamp is not None
        assert result.batch_start_timestamp <= result.batch_end_timestamp

    def test_a_batch_that_published_nothing_reports_no_window(self, runner_factory):
        runner, _ = runner_factory([])
        result = runner.run_batch()

        assert result.counters.acked == 0
        assert result.batch_start_timestamp is None
        assert result.batch_end_timestamp is None
        assert result.published_trigger_subtype is None

    def test_a_quarantined_record_does_not_widen_the_window(self, runner_factory):
        """Only records that reached the broker count: the window is what TBB
        is told was produced."""
        poison = {"region": "EMEA"}
        runner, _ = runner_factory([VALID_ROW, poison])
        result = runner.run_batch()

        assert result.counters.quarantined == 1
        assert result.counters.acked == 1
        assert result.batch_start_timestamp == result.batch_end_timestamp


class TestTheMonthIsOneBatch:
    """One Athena query returns the whole month; every row of it is published."""

    def _rows(self, n):
        return [row(counterparty_csid_sds=9900000000 + i) for i in range(n)]

    def test_every_row_of_the_month_is_published(self):
        runner, producer, _ = build_runner(self._rows(25))
        result = runner.run_batch()

        assert result.outcome == "SUCCESS"
        assert result.counters.records_parsed == 25
        assert result.counters.acked == 25
        assert len(producer.produced) == 25
        assert result.reconciliation.balanced
        assert result.source == {
            "table": "ifc_trigger_db.trigger_8",
            "business_date": "2026-06-30",
            "query_execution_id": "q-1",
        }

    def test_the_run_trigger_table_is_queried_for_the_month_end(self):
        athena = FakeAthena([VALID_ROW])
        runner, _, _ = build_runner([], athena=athena)
        runner.run_batch()

        (query,) = athena.queries
        assert '"ifc_trigger_db"."trigger_8"' in query["QueryString"]
        assert query["ExecutionParameters"] == ["'2026-06-30'"]

    def test_a_month_spanning_several_result_pages_is_published_whole(self):
        """Hundreds a month in practice; paging must still not drop a row."""
        runner, _, _ = build_runner(self._rows(2_500))
        result = runner.run_batch()

        assert result.counters.acked == 2_500
        assert result.reconciliation.balanced
        assert result.published_trigger_subtype == "NewHRCRelationship"

    def test_a_bad_row_is_quarantined_and_the_rest_of_the_month_publishes(self):
        rows = self._rows(5)
        rows[2] = {"region": "EMEA"}  # no CSID
        runner, _, _ = build_runner(rows)
        result = runner.run_batch()

        assert result.counters.quarantined == 1
        assert result.counters.acked == 4
        assert result.reconciliation.balanced

    def test_a_failed_query_fails_the_run_rather_than_reporting_success(self):
        runner, _, _ = build_runner([], athena=FakeAthena(self._rows(3), state="FAILED"))
        result = runner.run_batch()

        assert result.outcome == "SOURCE_UNREADABLE"
        assert result.exit_code != catalog.EXIT_OK
        assert result.counters.acked == 0
        assert result.classification is not None
        assert result.classification.scenario.key == catalog.BDP_READ_FAILURE.key

    def test_an_empty_month_is_reported_as_zero_records(self):
        """Distinct from a failed query: the query ran and found nothing."""
        runner, _, _ = build_runner([])
        result = runner.run_batch()

        assert result.outcome == "ZERO_RECORDS"
        assert result.counters.acked == 0
