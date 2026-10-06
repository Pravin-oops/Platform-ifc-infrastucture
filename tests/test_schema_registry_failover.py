"""Several Schema Registry URLs: every node is checked, and the lookup fails over."""

from __future__ import annotations

import json
import os
import socket
from typing import Any, Dict, List

import pytest
import requests

from utility import failure_catalog as catalog
from utility import kafka_preflight as pf
from utility import schema_registry_client as src
from utility.connector_config import SchemaRegistrySettings, load_settings
from utility.error_classifier import ConnectorError
from utility.kafka_factory import KafkaStackFactory
from utility.resilience_utility import BackoffPolicy, ShutdownSignal

CONFIG = os.path.join(os.path.dirname(__file__), "..", "utility", "connector_config.yaml")

A = "https://registry-a.example:8095"
B = "https://registry-b.example:8095"
SUBJECT = "topic-value"
DOCUMENT = {"id": 42, "version": 3, "schema": json.dumps({"type": "record", "name": "R", "fields": []})}


class Response:
    def __init__(self, status_code: int, body: Any = None):
        self.status_code = status_code
        self._body = body
        self.text = json.dumps(body)

    def json(self) -> Any:
        return self._body


class Tokens:
    def get(self, *, force_refresh: bool = False) -> str:
        return "token"


@pytest.fixture
def nodes(monkeypatch):
    """What each node answers: a Response, or an exception to raise. Records the calls."""
    answers: Dict[str, Any] = {}
    calls: List[str] = []

    def get(url, **_kwargs):
        node = next(base for base in answers if url.startswith(base))
        calls.append(node)
        answer = answers[node]
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(src.requests, "get", get)
    return answers, calls


def client(*urls: str, attempts: int = 1) -> src.SchemaRegistryClient:
    return src.SchemaRegistryClient(
        list(urls),
        token_provider=Tokens(),
        attempts=attempts,
        backoff=BackoffPolicy(base_seconds=0.001, max_seconds=0.001),
    )


class TestSettings:
    def test_a_comma_separated_url_gives_every_node(self):
        settings = SchemaRegistrySettings(url=f"{A}, {B}/")

        assert settings.urls == [A, B]

    def test_a_yaml_list_is_accepted(self):
        assert SchemaRegistrySettings(url=[A, B]).urls == [A, B]

    def test_a_single_url_still_works(self):
        assert SchemaRegistrySettings(url=A).urls == [A]

    def test_a_malformed_entry_is_rejected(self):
        with pytest.raises(ValueError, match="must be http"):
            SchemaRegistrySettings(url=f"{A}, registry-b:8095")


class TestFailover:
    def test_the_first_node_answers(self, nodes):
        answers, calls = nodes
        answers.update({A: Response(200, DOCUMENT), B: Response(200, DOCUMENT)})

        registered = client(A, B).latest_schema(SUBJECT)

        assert (registered.schema_id, registered.url) == (42, A)
        assert calls == [A]

    @pytest.mark.parametrize(
        "outage",
        [requests.ConnectionError("refused"), requests.exceptions.SSLError("bad cert"), Response(503, {})],
    )
    def test_an_unavailable_node_fails_over_to_the_next(self, nodes, outage):
        answers, calls = nodes
        answers.update({A: outage, B: Response(200, DOCUMENT)})

        registered = client(A, B).latest_schema(SUBJECT)

        assert (registered.schema_id, registered.url) == (42, B)
        assert calls == [A, B]

    def test_the_node_that_answered_is_tried_first_next_time(self, nodes):
        answers, calls = nodes
        answers.update({A: requests.ConnectionError("refused"), B: Response(200, DOCUMENT)})
        registry = client(A, B)

        registry.latest_schema(SUBJECT)
        registry.latest_schema(SUBJECT)

        assert calls == [A, B, B]

    @pytest.mark.parametrize(
        "status, scenario",
        [
            (401, catalog.AUTHENTICATION_FAILURE),
            (403, catalog.AUTHORISATION_FAILURE),
            (404, catalog.SCHEMA_VALIDATION_FAILURE),
        ],
    )
    def test_a_rejection_is_the_clusters_answer_and_is_not_failed_over(self, nodes, status, scenario):
        answers, calls = nodes
        answers.update({A: Response(status, {}), B: Response(200, DOCUMENT)})

        with pytest.raises(ConnectorError) as exc:
            client(A, B).latest_schema(SUBJECT)

        assert exc.value.scenario is scenario
        assert B not in calls

    def test_every_node_down_is_a_registry_outage_naming_them_all(self, nodes):
        answers, calls = nodes
        answers.update({A: requests.ConnectionError("refused"), B: Response(502, {})})

        with pytest.raises(ConnectorError, match="No Schema Registry node answered") as exc:
            client(A, B, attempts=2).latest_schema(SUBJECT)

        assert exc.value.scenario is catalog.SCHEMA_REGISTRY_UNAVAILABLE
        assert A in str(exc.value) and B in str(exc.value)
        # Each retry attempt goes round every node again.
        assert calls == [A, B, A, B]


class TestPreflight:
    """Every node is checked and reported; only all of them down fails the run."""

    @pytest.fixture
    def network(self, monkeypatch):
        down: List[str] = []

        def resolve(host, *_args, **_kwargs):
            if host in down:
                raise socket.gaierror("Name or service not known")
            return [(None, None, None, None, ("10.0.0.1", 0))]

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return False

        def connect(address, timeout):
            if address[0] in down:
                raise OSError("unreachable")
            return Connection()

        monkeypatch.setattr(pf.socket, "getaddrinfo", resolve)
        monkeypatch.setattr(pf.socket, "create_connection", connect)
        return down

    def run(self, urls: List[str]) -> pf.PreflightReport:
        """The factory's own network checks, with the registry URLs swapped in."""
        settings = load_settings(CONFIG)
        settings.schema_registry.url = ", ".join(urls)
        factory = KafkaStackFactory(settings, metrics=None, shutdown=ShutdownSignal())
        factory._network_checks({"bootstrap.servers": "broker.example:9092"})
        return factory.report

    def test_one_node_down_is_reported_but_does_not_fail(self, network):
        network.append("registry-b.example")

        report = self.run([A, B])

        assert report.passed
        failed = [r.name for r in report.results if not r.passed]
        checked = [r.name for r in report.results if "schema_registry" in r.name]
        assert "tcp:schema_registry:registry-a.example:8095" in checked
        assert failed == [
            "dns:schema_registry:registry-b.example",
            "tcp:schema_registry:registry-b.example:8095",
        ]

    def test_every_node_down_fails(self, network):
        network.extend(["registry-a.example", "registry-b.example"])

        assert not self.run([A, B]).passed

    def test_a_single_node_must_be_reachable(self, network):
        network.append("registry-a.example")

        assert not self.run([A]).passed


class TestBrokerDiagnostics:
    """A failed metadata request names why each broker connection was dropped."""

    SSL_ERROR = (
        'KafkaError{code=_SSL,val=-181,str="sasl_ssl://broker:9092/bootstrap: SSL handshake '
        'failed: error:0A00010B:SSL routines::wrong version number"}'
    )

    class Producer:
        def __init__(self, config, errors):
            self._config = config
            self._errors = errors

        def list_topics(self, topic, timeout):
            raise RuntimeError('KafkaError{code=_TRANSPORT,val=-195,str="Failed to get metadata"}')

        def poll(self, timeout):
            # librdkafka serves error_cb from poll().
            for error in self._errors:
                self._config["error_cb"](error)

    def factory(self, errors):
        settings = load_settings(CONFIG)
        factory = KafkaStackFactory(settings, metrics=None, shutdown=ShutdownSignal())
        config = {"bootstrap.servers": "broker:9092", "security.protocol": "SASL_SSL"}
        from utility.kafka_factory import _BrokerErrors

        config["error_cb"] = factory._broker_errors = _BrokerErrors(None)
        factory._metadata_checks(self.Producer(config, errors))
        return factory.report.results[-1]

    def test_the_tls_reason_is_reported_and_filed_as_authentication(self):
        result = self.factory([self.SSL_ERROR, 'KafkaError{code=_ALL_BROKERS_DOWN,str="1/1 brokers are down"}'])

        assert not result.passed
        assert "wrong version number" in result.detail
        assert "brokers are down" not in result.detail
        assert result.scenario is catalog.AUTHENTICATION_FAILURE

    def test_without_broker_errors_it_is_still_a_broker_failure(self):
        result = self.factory([])

        assert not result.passed
        assert result.scenario is catalog.BROKER_UNAVAILABLE

    def test_a_callback_from_the_bsp_config_is_still_called(self):
        from utility.kafka_factory import _BrokerErrors

        seen = []
        errors = _BrokerErrors(seen.append)
        errors("boom")

        assert seen == ["boom"] and errors.recent == ["boom"]


class TestLibrdkafkaDebug:
    """kafka.debug routes librdkafka's trace into the logs and keeps what led to a failure."""

    def test_debug_is_routed_into_the_logs_at_level_7(self):
        settings = load_settings(CONFIG)
        settings.kafka.debug = "security,broker,protocol"
        factory = KafkaStackFactory(settings, metrics=None, shutdown=ShutdownSignal())

        config = factory._producer_config(None)

        assert set(config["debug"].split(",")) == {"security", "broker", "protocol"}
        assert config["log_level"] == 7
        assert config["logger"].name == "librdkafka"

    def test_without_debug_nothing_is_routed(self):
        factory = KafkaStackFactory(load_settings(CONFIG), metrics=None, shutdown=ShutdownSignal())

        config = factory._producer_config(None)

        assert "debug" not in config and "logger" not in config

    @pytest.mark.parametrize("contexts", ["all", "security,conf"])
    def test_contexts_that_print_the_config_are_refused(self, contexts):
        settings = load_settings(CONFIG)

        with pytest.raises(ValueError, match="must not include"):
            type(settings.kafka)(**{**settings.kafka.model_dump(), "debug": contexts})

    def test_key_lines_skip_lines_that_only_name_a_sasl_ssl_broker(self):
        from utility.kafka_factory import _LibrdkafkaTrace

        trace = _LibrdkafkaTrace()
        for line in [
            "INIT [rdkafka#producer-1] [thrd:app]: librdkafka initialized (builtin.features ssl,sasl)",
            "CONNECT [rdkafka#producer-1] [thrd:sasl_ssl://b:9095/bootstrap]: sasl_ssl://b:9095/bootstrap: Connecting",
            "SSL [rdkafka#producer-1] [thrd:sasl_ssl://b:9095/bootstrap]: sasl_ssl://b:9095/bootstrap: certificate verify failed",
            "SASL [rdkafka#producer-1] [thrd:sasl_ssl://b:9095/bootstrap]: sasl_ssl://b:9095/bootstrap: Send SASL OAUTHBEARER frame",
        ]:
            trace.lines.append(line)

        assert trace.key_lines(10) == trace_lines_about_tls_and_sasl(trace)


def trace_lines_about_tls_and_sasl(trace):
    return [line for line in trace.lines if line.startswith(("SSL ", "SASL "))]


class TestObservedOauthCallback:
    """What the BSP oauth_cb hands librdkafka is logged, never the token."""

    @staticmethod
    def token(sub="svc@REALM"):
        import base64 as b64

        claims = b64.urlsafe_b64encode(json.dumps({"sub": sub, "exp": 1}).encode()).decode().rstrip("=")
        return f"e30.{claims}.secret-signature"

    def run(self, caplog, result):
        import logging as logging_module

        from utility.kafka_factory import _observed_oauth_cb

        with caplog.at_level(logging_module.INFO, logger="utility.kafka_factory"):
            returned = _observed_oauth_cb(lambda _config: result)("cfg")
        return returned, caplog.records

    def test_a_valid_token_is_passed_through_and_described(self, caplog):
        import time as time_module

        result = (self.token(), time_module.time() + 3600)
        returned, records = self.run(caplog, result)

        assert returned is result
        described = records[-1]
        assert "expires_in_seconds=3600" in described.getMessage() or "expires_in_seconds=3599" in described.getMessage()
        assert described.oauth_token_claims["sub"] == "svc@REALM"
        assert "secret-signature" not in " ".join(r.getMessage() for r in records)

    def test_an_expired_token_is_an_error(self, caplog):
        import time as time_module

        _, records = self.run(caplog, (self.token(), time_module.time() - 10))

        assert any(r.levelname == "ERROR" and "already expired" in r.getMessage() for r in records)

    def test_a_millisecond_expiry_is_flagged(self, caplog):
        import time as time_module

        _, records = self.run(caplog, (self.token(), time_module.time() * 1000))

        assert any(r.levelname == "WARNING" and "milliseconds" in r.getMessage() for r in records)

    def test_a_failing_callback_is_logged_and_still_raises(self, caplog):
        from utility.kafka_factory import _observed_oauth_cb

        def broken(_config):
            raise RuntimeError("BAM unreachable")

        with pytest.raises(RuntimeError, match="BAM unreachable"):
            _observed_oauth_cb(broken)("cfg")
        assert any("oauth_cb raised RuntimeError" in r.getMessage() for r in caplog.records)
