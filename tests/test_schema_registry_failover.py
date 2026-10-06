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
