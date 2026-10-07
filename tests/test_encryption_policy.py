"""fieldEncryptionPolicy is declared only in PROD, where the upstream data is tokenised."""

from __future__ import annotations

import json
import os

import pytest

from tests.test_envelope import trigger_8_event
from utility.connector_config import load_settings
from utility.connector_utility import load_schema_document
from utility.tb_outcome_schema import EnvelopeBuilder

CONFIG = os.path.join(os.path.dirname(__file__), "..", "utility", "connector_config.yaml")
OWNER = "Client Relationship Owner Name"


def policies(declare: bool) -> dict:
    builder = EnvelopeBuilder(
        avro_schema=load_schema_document("utility/schema.json"),
        originating_system="SNSVC0084373",
        declare_encryption_policies=declare,
        business_month="2026-09",
    )
    payload = json.loads(builder.build(trigger_8_event()).record["payload"])
    return {field["fieldName"]: field["fieldEncryptionPolicy"] for field in payload}


@pytest.fixture
def settings_for(monkeypatch):
    def load(environment: str):
        monkeypatch.setenv("IFC_APP__ENVIRONMENT", environment)
        return load_settings(CONFIG)

    return load


@pytest.mark.parametrize("environment", ["PROD", "prod"])
def test_prod_declares_the_policy(settings_for, environment):
    assert settings_for(environment).declares_encryption_policies is True


@pytest.mark.parametrize("environment", ["DEV", "SIT", "PROD-ANALYTICS", "PROD-PARALLEL"])
def test_every_other_environment_declares_none(settings_for, environment):
    assert settings_for(environment).declares_encryption_policies is False


def test_declared_the_owner_name_carries_the_tokenisation_policy_and_nothing_else_does():
    declared = policies(True)

    assert declared.pop(OWNER) == "UK_TOK_AC_L0R0_UNC_DE"
    assert set(declared.values()) == {""}


def test_not_declared_every_field_is_empty():
    assert set(policies(False).values()) == {""}


def test_a_builder_must_be_told_whether_to_declare():
    with pytest.raises(TypeError, match="declare_encryption_policies"):
        EnvelopeBuilder(avro_schema=load_schema_document("utility/schema.json"), originating_system="SNSVC0084373")


@pytest.mark.parametrize("environment, declared", [("PROD", True), ("SIT", False)])
def test_the_runner_builds_envelopes_for_its_environment(environment, declared):
    """Through start() and the real factory, as a task would run."""
    from tests.test_runner_pipeline import FakeProducer, make_settings
    from utility.connector_runner import ConnectorRunner
    from utility.resilience_utility import ShutdownSignal

    settings = make_settings(
        app={"environment": environment},
        envelope={"originating_systems": {environment: "SNSVC0084373"}, "tokenised_environments": ["PROD"]},
        schema_registry={"mode": "DEV", "schema_id": 1299},
        kafka={"bsp_config_path": None, "overrides": {"bootstrap.servers": "localhost:9092"}},
    )
    runner = ConnectorRunner(settings, shutdown=ShutdownSignal(), producer_factory=lambda _c: FakeProducer())

    runner.start()

    assert runner._envelopes._declare_policies is declared
