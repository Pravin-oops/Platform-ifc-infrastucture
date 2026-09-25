"""Failure classification, resilience primitives and reconciliation."""

from __future__ import annotations

import time

import pytest

from ifc_trigger_connector.utility.audit_utility import RunCounters, reconcile
from ifc_trigger_connector.utility import failure_catalog as catalog
from ifc_trigger_connector.utility.failure_catalog import Handling, Layer
from ifc_trigger_connector.utility.error_classifier import (
    ConnectorError,
    RecordRejected,
    classify,
)
from ifc_trigger_connector.utility.failure_notifier import build_body, build_subject
from ifc_trigger_connector.utility.connector_utility import SourceAccessError
from ifc_trigger_connector.utility.kafka_serializers import SizeGuard
from ifc_trigger_connector.utility.resilience_utility import BackoffPolicy, CircuitBreaker, CircuitOpen, ShutdownSignal, retry


class FakeKafkaError:
    """Stands in for confluent_kafka.KafkaError, which exposes methods not attributes."""

    def __init__(self, name: str, code: int = -1, message: str = ""):
        self._name = name
        self._code = code
        self._message = message or name

    def name(self):
        return self._name

    def code(self):
        return self._code

    def __str__(self):
        return self._message


class TestCatalogue:
    def test_every_layer_from_the_agreed_matrix_is_represented(self):
        grouped = catalog.scenarios_by_layer()
        for layer in Layer:
            assert grouped[layer], f"no scenarios for {layer}"

    def test_exit_codes_are_unique_per_scenario(self):
        codes = [s.exit_code for s in catalog.SCENARIOS.values() if s.key != "UNKNOWN"]
        assert len(codes) == len(set(codes))

    def test_exit_codes_avoid_the_reserved_drain_code(self):
        assert catalog.EXIT_WORK_REMAINING not in {s.exit_code for s in catalog.SCENARIOS.values()}

    def test_scenarios_handled_in_the_connector_declare_how(self):
        for scenario in catalog.SCENARIOS.values():
            if scenario.handled_in_connector:
                assert scenario.handling is not Handling.NONE
                assert scenario.connector_behaviour

    def test_upstream_scenarios_are_honest_about_not_being_handled(self):
        assert catalog.FRED_PROCESSING_FAILURE.handling is Handling.NONE
        assert catalog.BDP_WRITE_FAILURE.handling is Handling.NONE


class TestClassifier:
    @pytest.mark.parametrize(
        "error,expected",
        [
            (FakeKafkaError("_MSG_SIZE_TOO_LARGE"), "MESSAGE_TOO_LARGE"),
            (FakeKafkaError("TOPIC_AUTHORIZATION_FAILED"), "AUTHORISATION_FAILURE"),
            (FakeKafkaError("UNKNOWN_TOPIC_OR_PART"), "TOPIC_UNAVAILABLE"),
            (FakeKafkaError("_ALL_BROKERS_DOWN"), "BROKER_UNAVAILABLE"),
            (FakeKafkaError("LEADER_NOT_AVAILABLE"), "KAFKA_PARTITION_LEADER_FAILURE"),
            (FakeKafkaError("_MSG_TIMED_OUT"), "HIGH_KAFKA_PUBLISH_LATENCY"),
            (RuntimeError("SaslAuthenticationException: bad credentials"), "AUTHENTICATION_FAILURE"),
            (RuntimeError("leader not available"), "KAFKA_PARTITION_LEADER_FAILURE"),
            (RuntimeError("getaddrinfo failed"), "NETWORK_CONNECTIVITY_FAILURE"),
            (MemoryError(), "PRODUCER_OUT_OF_MEMORY"),
        ],
    )
    def test_known_signatures_map_to_the_right_scenario(self, error, expected):
        assert classify(error).scenario.key == expected

    def test_separator_variants_match_the_same_pattern(self):
        assert classify(RuntimeError("NotLeaderForPartition")).scenario.key == (
            classify(RuntimeError("NOT_LEADER_FOR_PARTITION")).scenario.key
        )

    def test_an_unrecognised_error_is_unknown_rather_than_misfiled(self):
        assert classify(RuntimeError("something entirely new")).scenario.key == "UNKNOWN"

    def test_an_already_classified_error_is_trusted(self):
        error = ConnectorError("x", catalog.TOPIC_UNAVAILABLE, context={"a": 1})
        result = classify(error, context={"b": 2})
        assert result.scenario is catalog.TOPIC_UNAVAILABLE
        assert result.context == {"a": 1, "b": 2}

    def test_s3_read_and_write_failures_route_to_different_owners(self):
        read = classify(SourceAccessError("denied", path="s3://b/k", operation="read"))
        write = classify(SourceAccessError("denied", path="s3://b/k", operation="write"))
        assert read.scenario.key == "TRIGGER_BDP_READ_FAILURE"
        assert write.scenario.key == "TRIGGER_BDP_WRITE_FAILURE"

    def test_nested_cause_is_searched(self):
        inner = RuntimeError("_ALL_BROKERS_DOWN")
        outer = RuntimeError("publish failed")
        outer.__cause__ = inner
        assert classify(outer).scenario.key == "BROKER_UNAVAILABLE"


class TestNotifier:
    def test_subject_fits_the_sns_limit_and_names_the_scenario(self):
        classification = classify(FakeKafkaError("_ALL_BROKERS_DOWN"))
        subject = build_subject(classification, environment="UAT")
        assert len(subject) <= 100
        assert "UAT" in subject and "CRITICAL" in subject

    def test_body_carries_ownership_and_the_agreed_actions(self):
        classification = classify(FakeKafkaError("TOPIC_AUTHORIZATION_FAILED"))
        body = build_body(classification, run_context={"run_id": "r1"})
        assert "TBB RTB" in body
        assert "ACTIONS" in body
        assert "run_id = r1" in body


class TestSizeGuard:
    def test_a_message_under_the_limit_passes(self):
        SizeGuard(1024).check(b"x" * 100, trigger_id="T", key="C1", record={})

    def test_an_oversized_message_is_rejected_before_produce(self):
        guard = SizeGuard(256)
        with pytest.raises(RecordRejected) as exc:
            guard.check(b"x" * 512, trigger_id="T", key="C1", record={"payload": "y" * 400})
        assert exc.value.scenario.key == "MESSAGE_TOO_LARGE"
        assert exc.value.detail["serialised_bytes"] == 512 + len("C1")

    def test_the_key_counts_toward_the_limit(self):
        guard = SizeGuard(10)
        with pytest.raises(RecordRejected):
            guard.check(b"x" * 8, trigger_id="T", key="12345", record={})


class TestBackoff:
    def test_delay_grows_and_is_capped(self):
        policy = BackoffPolicy(base_seconds=1, max_seconds=8, jitter=False)
        assert [policy.delay(n) for n in range(1, 6)] == [1, 2, 4, 8, 8]

    def test_jitter_keeps_delays_inside_the_bound(self):
        policy = BackoffPolicy(base_seconds=1, max_seconds=4, jitter=True)
        assert all(0 <= policy.delay(3) <= 4 for _ in range(50))

    def test_retry_gives_up_on_a_non_retryable_error_immediately(self):
        calls = []

        def operation():
            calls.append(1)
            raise ValueError("permanent")

        with pytest.raises(ValueError):
            retry(
                operation,
                attempts=5,
                policy=BackoffPolicy(base_seconds=0.001),
                retry_on=lambda exc: False,
                description="test",
            )
        assert len(calls) == 1

    def test_retry_succeeds_after_transient_failures(self):
        state = {"n": 0}

        def operation():
            state["n"] += 1
            if state["n"] < 3:
                raise RuntimeError("transient")
            return "ok"

        result = retry(
            operation,
            attempts=5,
            policy=BackoffPolicy(base_seconds=0.001, max_seconds=0.002),
            retry_on=lambda exc: True,
            description="test",
        )
        assert result == "ok" and state["n"] == 3

    def test_shutdown_during_backoff_aborts_the_retry(self):
        shutdown = ShutdownSignal()
        shutdown.set("test")

        with pytest.raises(RuntimeError):
            retry(
                lambda: (_ for _ in ()).throw(RuntimeError("transient")),
                attempts=5,
                policy=BackoffPolicy(base_seconds=5),
                retry_on=lambda exc: True,
                shutdown=shutdown,
                description="test",
            )


class TestCircuitBreaker:
    def test_opens_after_consecutive_failures(self):
        breaker = CircuitBreaker(threshold=3, reset_seconds=60)
        for _ in range(3):
            breaker.record_failure(RuntimeError("down"))
        with pytest.raises(CircuitOpen):
            breaker.raise_if_open()

    def test_a_success_resets_the_count(self):
        breaker = CircuitBreaker(threshold=3, reset_seconds=60)
        breaker.record_failure()
        breaker.record_failure()
        breaker.record_success()
        breaker.record_failure()
        breaker.raise_if_open()

    def test_half_opens_after_the_cooldown(self):
        breaker = CircuitBreaker(threshold=1, reset_seconds=0.05)
        breaker.record_failure()
        assert breaker.is_open
        time.sleep(0.06)
        assert not breaker.is_open


class TestReconciliation:
    def test_a_balanced_run_passes(self):
        counters = RunCounters(records_parsed=10, published=8, quarantined=2, acked=8)
        assert reconcile(counters).balanced

    def test_an_unexplained_gap_fails(self):
        counters = RunCounters(records_parsed=10, published=8, acked=8)
        result = reconcile(counters)
        assert not result.balanced
        assert "does not balance" in result.findings[0]

    def test_unacknowledged_messages_fail_even_when_the_count_balances(self):
        counters = RunCounters(records_parsed=10, published=10, acked=9)
        result = reconcile(counters)
        assert not result.balanced
        assert any("not acknowledged" in f for f in result.findings)

    def test_an_uncertain_flush_is_reported_as_uncertain(self):
        counters = RunCounters(records_parsed=5, published=5, acked=5, unflushed=2)
        assert any("uncertain" in f for f in reconcile(counters).findings)

    def test_parse_failures_are_accounted_for(self):
        counters = RunCounters(records_parsed=8, parse_failures=2, published=8, acked=8)
        assert reconcile(counters).balanced
