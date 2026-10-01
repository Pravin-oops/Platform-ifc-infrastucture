"""End-to-end batch behaviour, with Kafka and the Schema Registry faked out.

These are the tests that actually exercise the ECS-specific behaviour: draining
on SIGTERM, suppressing duplicates across a restart, quarantining a poison
record without losing the batch, and refusing to report success when the numbers
do not add up.
"""

from __future__ import annotations

import json
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

VALID_EVENT = {
    "triggerSubType": "TRIGGER_8",
    "attributes": {
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
    },
}


def write_events(directory, events, name="events.jsonl"):
    path = directory / name
    path.write_text("\n".join(json.dumps(e) for e in events), encoding="utf-8")
    return path


def make_settings(source_path, **overrides) -> ConnectorSettings:
    document = {
        "app": {"environment": "TEST", "log_level": "WARNING"},
        "run": {"mode": "batch", "shutdown_grace_seconds": 5},
        "source": {"type": "local", "path": str(source_path)},
        "kafka": {
            "topic": "test_ifc_topic",
            "bsp_config_path": "config/bsp_local_config.yaml",
        },
        "schema_registry": {"mode": "DEV"},
        "state": {"backend": "memory"},
        "audit": {"bucket": None},
        "resilience": {"preflight_enabled": False, "max_quarantine_ratio": 1.0},
        "health": {"enabled": False},
    }
    for section, values in overrides.items():
        document.setdefault(section, {}).update(values)
    return ConnectorSettings.model_validate(document)


@pytest.fixture
def runner_factory(tmp_path):
    def make(events, *, fail_keys=None, **overrides):
        write_events(tmp_path, events)
        settings = make_settings(tmp_path, **overrides)
        metrics = Metrics()
        producer = FakeProducer(fail_keys=fail_keys)

        runner = ConnectorRunner(settings, metrics=metrics, shutdown=ShutdownSignal())
        runner._stack = build_stack(producer, settings, metrics)

        from utility.tb_outcome_schema import EnvelopeBuilder

        runner._envelopes = EnvelopeBuilder(
            avro_schema=runner._stack.serializer.schema,
            sequence_allocator=runner._sequence,
            business_month="2026-06",
        )
        return runner, producer

    return make


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_a_valid_batch_publishes_and_reconciles(self, runner_factory):
        runner, producer = runner_factory([VALID_EVENT])
        result = runner.run_batch()

        assert result.outcome == "SUCCESS"
        assert result.exit_code == catalog.EXIT_OK
        assert result.counters.published == 1
        assert result.counters.acked == 1
        assert result.reconciliation.balanced
        assert producer.produced[0]["topic"] == "test_ifc_topic"

    def test_the_record_carries_no_headers(self, runner_factory):
        runner, producer = runner_factory([VALID_EVENT])
        runner.run_batch()

        assert not producer.produced[0]["headers"]

    def test_the_wire_format_is_magic_byte_plus_schema_id(self, runner_factory):
        runner, producer = runner_factory([VALID_EVENT])
        runner.run_batch()

        value = producer.produced[0]["value"]
        assert value[0] == 0
        assert int.from_bytes(value[1:5], "big") == 101


class TestRunLogs:
    """The lines an operator reads in the ECS console to answer 'did it publish?'."""

    def messages(self, caplog, prefix):
        return [r.getMessage() for r in caplog.records if r.getMessage().startswith(prefix)]

    def test_each_acknowledged_message_is_logged_as_published(self, runner_factory, caplog):
        caplog.set_level("INFO")
        runner, _ = runner_factory([VALID_EVENT])
        runner.run_batch()

        [published] = self.messages(caplog, "Message published:")
        assert "topic=test_ifc_topic" in published
        assert f"trigger_id={FIRST_TRIGGER_ID}" in published
        assert "offset=1" in published

    def test_a_failed_delivery_is_logged_as_not_published(self, runner_factory, caplog):
        caplog.set_level("INFO")
        runner, _ = runner_factory([VALID_EVENT], fail_keys={FIRST_TRIGGER_ID})
        runner.run_batch()

        assert self.messages(caplog, "Message published:") == []
        [failed] = self.messages(caplog, "Message NOT published:")
        assert f"trigger_id={FIRST_TRIGGER_ID}" in failed

    def test_the_batch_ends_with_a_publish_summary(self, runner_factory, caplog):
        caplog.set_level("INFO")
        runner, _ = runner_factory([VALID_EVENT])
        runner.run_batch()

        [summary] = self.messages(caplog, "Kafka publish summary:")
        assert "topic=test_ifc_topic schema_id=101 published=1 acked=1 delivery_failed=0" in summary


class TestQuarantine:
    def test_a_poison_record_is_quarantined_and_the_batch_continues(self, runner_factory):
        poison = {**VALID_EVENT, "attributes": {"region": "EMEA"}}
        runner, producer = runner_factory([poison, VALID_EVENT])

        result = runner.run_batch()

        assert result.counters.quarantined == 1
        assert result.counters.published == 1
        assert result.reconciliation.balanced

    def test_an_unparsable_line_is_counted_and_does_not_stop_the_batch(self, runner_factory, tmp_path):
        runner, producer = runner_factory([VALID_EVENT])
        (tmp_path / "events.jsonl").write_text(
            "{not json}\n" + json.dumps(VALID_EVENT), encoding="utf-8"
        )

        result = runner.run_batch()

        assert result.counters.parse_failures == 1
        assert result.counters.published == 1
        assert result.reconciliation.balanced

    def test_exceeding_the_quarantine_tolerance_fails_the_run(self, runner_factory):
        poison = {**VALID_EVENT, "attributes": {}}
        runner, _ = runner_factory(
            [poison, VALID_EVENT], resilience={"max_quarantine_ratio": 0.1}
        )

        result = runner.run_batch()

        assert result.outcome == "QUALITY_GATE_FAILED"
        assert result.exit_code == catalog.SCHEMA_VALIDATION_FAILURE.exit_code


class TestSequenceNumbers:
    """No durable state: the consuming team resolves duplicates, so a re-run
    republishes rather than the connector remembering what it sent."""

    def test_a_re_run_republishes_rather_than_suppressing(self, runner_factory):
        runner, producer = runner_factory([VALID_EVENT])
        runner.run_batch()
        assert len(producer.produced) == 1

        runner2, producer2 = runner_factory([VALID_EVENT])
        result = runner2.run_batch()

        assert result.counters.published == 1
        assert len(producer2.produced) == 1
        assert result.reconciliation.balanced

    def test_each_customer_starts_at_one(self, runner_factory):
        second = json.loads(json.dumps(VALID_EVENT))
        second["attributes"]["counterparty_csid_sds"] = 9912345679
        runner, producer = runner_factory([VALID_EVENT, second])
        result = runner.run_batch()

        assert result.counters.acked == 2
        assert runner._sequence.customers == 2
        assert runner._sequence.occurrences("9912345678") == 1
        assert runner._sequence.occurrences("9912345679") == 1

    def test_a_repeated_customer_counts_up(self, runner_factory):
        """Two events from one source for one customer: 1, then 2."""
        runner, producer = runner_factory([VALID_EVENT, json.loads(json.dumps(VALID_EVENT))])
        result = runner.run_batch()

        assert result.counters.acked == 2
        assert runner._sequence.customers == 1
        assert runner._sequence.occurrences("9912345678") == 2


class TestFailureOutcomes:
    def test_a_delivery_failure_fails_reconciliation(self, runner_factory):
        runner, _ = runner_factory([VALID_EVENT], fail_keys={FIRST_TRIGGER_ID})
        result = runner.run_batch()

        assert not result.reconciliation.balanced
        assert result.outcome == "RECONCILIATION_FAILED"
        assert result.exit_code == catalog.RECONCILIATION_FAILURE.exit_code

    def test_the_dominant_failure_scenario_is_reported(self, runner_factory):
        runner, _ = runner_factory([VALID_EVENT], fail_keys={FIRST_TRIGGER_ID})
        result = runner.run_batch()
        assert result.delivery["by_scenario"] == {"BROKER_UNAVAILABLE": 1}

    def test_an_empty_source_is_reported_as_zero_records_not_success(self, runner_factory, tmp_path):
        runner, _ = runner_factory([])
        (tmp_path / "events.jsonl").write_text("", encoding="utf-8")

        result = runner.run_batch()

        assert result.outcome == "ZERO_RECORDS"
        assert result.exit_code == catalog.TED_MISSING_SOURCE_DATA.exit_code


class TestShutdown:
    def test_a_signal_stops_intake_and_drains_what_was_queued(self, runner_factory):
        events = [
            {**VALID_EVENT, "attributes": {**VALID_EVENT["attributes"], "business_date": f"2026-06-{i % 28 + 1:02d}"}}
            for i in range(5)
        ]
        runner, producer = runner_factory(events)
        runner._shutdown.set("test")

        result = runner.run_batch()

        assert result.outcome == "DRAINED"
        assert result.exit_code == catalog.EXIT_WORK_REMAINING
        assert producer.produced == []

    def test_health_reports_draining_after_a_signal(self, runner_factory):
        runner, _ = runner_factory([VALID_EVENT])
        runner._shutdown.set("test")
        runner.run_batch()
        assert runner.health.snapshot()["draining"] is True


class TestBackpressure:
    def test_a_full_local_queue_is_waited_out_rather_than_grown(self, runner_factory, tmp_path):
        events = [
            {**VALID_EVENT, "attributes": {**VALID_EVENT["attributes"], "business_date": f"2026-06-{i % 28 + 1:02d}"}}
            for i in range(6)
        ]
        write_events(tmp_path, events)
        settings = make_settings(tmp_path)
        metrics = Metrics()

        # A queue that only holds two messages forces BufferError repeatedly.
        producer = FakeProducer(queue_limit=2)
        runner = ConnectorRunner(settings, metrics=metrics, shutdown=ShutdownSignal())
        runner._stack = build_stack(producer, settings, metrics)

        from utility.tb_outcome_schema import EnvelopeBuilder

        runner._envelopes = EnvelopeBuilder(
            avro_schema=runner._stack.serializer.schema,
            sequence_allocator=runner._sequence,
            business_month="2026-06",
        )

        result = runner.run_batch()

        assert result.counters.published == 6
        assert metrics.get("BackpressureWaits") > 0

class TestBatchCompletionWindow:
    """What the Trigger Backbone completion notification reports on."""

    def test_a_published_batch_reports_its_posting_window_and_sub_type(self, runner_factory):
        second = json.loads(json.dumps(VALID_EVENT))
        second["attributes"]["counterparty_csid_sds"] = 9912345679
        runner, _ = runner_factory([VALID_EVENT, second])
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
        poison = {"triggerSubType": "TRIGGER_8", "attributes": {"region": "EMEA"}}
        runner, _ = runner_factory([VALID_EVENT, poison])
        result = runner.run_batch()

        assert result.counters.quarantined == 1
        assert result.counters.acked == 1
        assert result.batch_start_timestamp == result.batch_end_timestamp


class TestTheSingleFileContract:
    """ECS is handed one .json object holding the whole batch as an array."""

    def _write_array(self, tmp_path, rows, name="batch.json"):
        path = tmp_path / name
        path.write_text(json.dumps(rows), encoding="utf-8")
        return path

    def _runner(self, path, **overrides):
        settings = make_settings(path, **overrides)
        metrics = Metrics()
        producer = FakeProducer()
        runner = ConnectorRunner(settings, metrics=metrics, shutdown=ShutdownSignal())
        runner._stack = build_stack(producer, settings, metrics)

        from utility.tb_outcome_schema import EnvelopeBuilder

        runner._envelopes = EnvelopeBuilder(
            avro_schema=runner._stack.serializer.schema,
            sequence_allocator=runner._sequence,
            business_month="2026-06",
        )
        return runner, producer

    def _rows(self, n):
        rows = []
        for i in range(n):
            row = json.loads(json.dumps(VALID_EVENT))
            row["attributes"]["counterparty_csid_sds"] = 9900000000 + i
            rows.append(row)
        return rows

    def test_one_file_of_records_publishes_all_of_them(self, tmp_path):
        path = self._write_array(tmp_path, self._rows(25))
        runner, producer = self._runner(path)
        result = runner.run_batch()

        assert result.outcome == "SUCCESS"
        assert result.counters.records_parsed == 25
        assert result.counters.acked == 25
        assert len(producer.produced) == 25
        assert result.reconciliation.balanced
        assert result.source_objects == [str(path)]
        assert result.counters.objects_read == 1

    def test_a_batch_larger_than_the_old_default_cap_is_published_whole(self, tmp_path):
        """5000 was the old default; a cap would now silently drop the tail."""
        path = self._write_array(tmp_path, self._rows(5_200))
        runner, _ = self._runner(path)
        result = runner.run_batch()

        assert result.counters.acked == 5_200
        assert result.reconciliation.balanced
        # The whole file is one batch, so the notification covers all of it.
        assert result.published_trigger_subtype == "NewHRCRelationship"

    def test_a_bad_record_is_quarantined_and_the_rest_of_the_file_publishes(self, tmp_path):
        rows = self._rows(5)
        rows[2] = {"triggerType": "IFC_CDD"}  # no triggerSubType
        path = self._write_array(tmp_path, rows)
        runner, _ = self._runner(path)
        result = runner.run_batch()

        assert result.counters.parse_failures == 1
        assert result.counters.acked == 4
        assert result.reconciliation.balanced

    def test_an_unreadable_source_fails_the_run_rather_than_reporting_success(self, tmp_path):
        """The hole the one-file contract opens: an unreadable object arrives as
        a single ParseFailure, which on its own reconciles and would have left a
        missing monthly file exiting 0."""
        path = self._write_array(tmp_path, self._rows(3))
        runner, _ = self._runner(path)

        # Stand in for S3 NoSuchKey: the object lists but cannot be read.
        from utility import trigger_source as source_module

        def unreadable(_path):
            raise source_module.SourceAccessError(
                "S3 read failed: NoSuchKey", path=str(path), operation="read"
            )

        original = source_module.read_text
        source_module.read_text = unreadable
        try:
            result = runner.run_batch()
        finally:
            source_module.read_text = original

        assert result.outcome == "SOURCE_UNREADABLE"
        assert result.exit_code != catalog.EXIT_OK
        assert result.counters.acked == 0
        assert result.classification is not None
        assert result.classification.scenario.key == catalog.BDP_READ_FAILURE.key

    def test_an_empty_array_is_reported_as_zero_records(self, tmp_path):
        """Distinct from unreadable: the file was there and held nothing."""
        path = self._write_array(tmp_path, [])
        runner, _ = self._runner(path)
        result = runner.run_batch()

        assert result.outcome == "ZERO_RECORDS"
        assert result.counters.acked == 0
