"""schema_registry works in SECURE and DEV: the config's mode, unless IFC_SCHEMA_REGISTRY__MODE says otherwise."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

import pytest

from utility import kafka_factory
from utility import schema_registry_client as src
from utility.connector_config import SchemaRegistrySettings, load_settings
from utility.connector_utility import load_schema_document
from utility.error_classifier import PreflightError
from utility.kafka_factory import KafkaStackFactory
from utility.resilience_utility import ShutdownSignal

CONFIG = os.path.join(os.path.dirname(__file__), "..", "utility", "connector_config.yaml")


@pytest.fixture
def registry(monkeypatch):
    """The shipped config's schema_registry, with IFC_SCHEMA_REGISTRY__MODE as given."""

    def load(mode=None):
        monkeypatch.delenv("IFC_SCHEMA_REGISTRY__MODE", raising=False)
        if mode is not None:
            monkeypatch.setenv("IFC_SCHEMA_REGISTRY__MODE", mode)
        return load_settings(CONFIG).schema_registry

    return load


def ports(settings: SchemaRegistrySettings) -> List[str]:
    return [url.rsplit(":", 1)[1] for url in settings.urls]


class TestModeSelection:
    def test_without_the_variable_the_config_file_decides(self, registry):
        settings = registry()

        assert settings.mode == "SECURE"
        assert ports(settings) == ["8095", "8095"]

    def test_an_empty_variable_also_leaves_it_to_the_config_file(self, registry):
        assert registry("").mode == "SECURE"

    @pytest.mark.parametrize("value", ["DEV", "dev", " Dev "])
    def test_the_variable_wins_in_any_case(self, registry, value):
        settings = registry(value)

        assert settings.mode == "DEV"
        assert ports(settings) == ["8082", "8082"]

    def test_the_variable_can_select_secure_over_a_dev_config(self, monkeypatch):
        with open(CONFIG, encoding="utf-8") as handle:
            shipped = handle.read()
        dev_config = shipped.replace("  mode: SECURE\n", "  mode: DEV\n", 1)
        assert dev_config != shipped

        monkeypatch.delenv("IFC_SCHEMA_REGISTRY__MODE", raising=False)
        assert load_settings("cfg.yaml", reader=lambda _p: dev_config).schema_registry.mode == "DEV"

        monkeypatch.setenv("IFC_SCHEMA_REGISTRY__MODE", "SECURE")
        settings = load_settings("cfg.yaml", reader=lambda _p: dev_config).schema_registry
        assert settings.mode == "SECURE"
        assert ports(settings) == ["8095", "8095"]

    def test_an_unknown_mode_is_refused(self, registry):
        with pytest.raises(ValueError, match="DEV' or 'SECURE"):
            registry("PROD")


class TestModeBlocks:
    def test_the_active_block_overrides_the_shared_values(self):
        settings = SchemaRegistrySettings.model_validate(
            {
                "mode": "SECURE",
                "timeout_seconds": 30,
                "ca_location": "/shared.pem",
                "secure": {"url": "https://r:8095", "timeout_seconds": 10},
                "dev": {"url": "https://r:8082", "ca_location": "/dev.pem"},
            }
        )

        assert (settings.urls, settings.timeout_seconds, settings.ca_location) == (
            ["https://r:8095"], 10, "/shared.pem",
        )

    def test_the_inactive_block_is_ignored(self):
        settings = SchemaRegistrySettings.model_validate(
            {"mode": "DEV", "dev": {"url": "https://r:8082"}, "secure": {"url": "https://r:8095", "timeout_seconds": 5}}
        )

        assert settings.urls == ["https://r:8082"]
        assert settings.timeout_seconds == 30  # the default, not the secure block's

    def test_a_flat_config_without_blocks_still_works(self):
        settings = SchemaRegistrySettings.model_validate({"mode": "SECURE", "url": "https://r:8095"})

        assert settings.urls == ["https://r:8095"]

    def test_secure_needs_a_url_from_the_block_or_the_shared_value(self):
        with pytest.raises(ValueError, match=r"schema_registry.secure.url"):
            SchemaRegistrySettings.model_validate({"mode": "SECURE", "dev": {"url": "https://r:8082"}})


class FakeClient:
    """Stands in for SchemaRegistryClient and records how it was built."""

    built: List[Dict[str, Any]] = []

    def __init__(self, urls, **kwargs):
        FakeClient.built.append({"urls": urls, **kwargs})

    def latest_schema(self, subject):
        schema = load_schema_document("utility/schema.json")
        return src.RegisteredSchema(subject=subject, schema_id=4242, version=1, schema=schema, url="https://r:8082")


class TestFactoryInDev:
    @pytest.fixture
    def factory(self, monkeypatch):
        from tests.test_runner_pipeline import make_settings

        FakeClient.built = []
        monkeypatch.setattr(kafka_factory, "SchemaRegistryClient", FakeClient)

        def build(**registry):
            settings = make_settings(schema_registry={"mode": "DEV", **registry})
            return KafkaStackFactory(settings, metrics=None, shutdown=ShutdownSignal())

        return build

    def test_dev_with_a_url_looks_the_schema_up_without_a_token(self, factory):
        _schema, schema_id, context = factory(dev={"url": "https://r:8082"})._resolve_schema(tokens="BAM")

        assert schema_id == 4242
        assert context["mode"] == "DEV"
        [client] = FakeClient.built
        assert client["urls"] == ["https://r:8082"]
        assert client["token_provider"] is None

    def test_secure_sends_the_bam_token(self, factory, monkeypatch):
        from tests.test_runner_pipeline import make_settings

        settings = make_settings(schema_registry={"mode": "SECURE", "secure": {"url": "https://r:8095"}})
        KafkaStackFactory(settings, metrics=None, shutdown=ShutdownSignal())._resolve_schema(tokens="BAM")

        assert FakeClient.built[-1]["token_provider"] == "BAM"

    def test_dev_with_a_pinned_id_does_not_ask_the_registry(self, factory):
        _schema, schema_id, context = factory(dev={"url": "https://r:8082", "schema_id": 1299})._resolve_schema(None)

        assert (schema_id, context) == (1299, {"mode": "DEV", "schema_id": 1299})
        assert FakeClient.built == []

    def test_dev_with_neither_refuses_to_start(self, factory):
        with pytest.raises(PreflightError, match="neither schema_registry.dev.url nor"):
            factory()._resolve_schema(None)


class TestClientWithoutAToken:
    def test_no_authorization_header_is_sent_and_a_401_is_not_retried(self, monkeypatch):
        seen: List[Dict[str, str]] = []

        class Response:
            status_code = 401
            text = "{}"

            def json(self):
                return {}

        def get(url, headers, **_kwargs):
            seen.append(headers)
            return Response()

        monkeypatch.setattr(src.requests, "get", get)
        client = src.SchemaRegistryClient("https://r:8082", attempts=1)

        with pytest.raises(Exception):
            client.latest_schema("t-value")

        assert len(seen) == 1
        assert "Authorization" not in seen[0]
        assert seen[0]["Content-Type"] == "application/vnd.schemaregistry.v1+json"
