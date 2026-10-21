"""fieldEncryptionPolicy is declared only in PROD, where the upstream data is tokenised."""

from __future__ import annotations

import json
import os

import pytest

from tests.test_envelope import trigger_8_event
from utility.connector_config import load_settings
from utility.connector_utility import load_schema_document
from utility.tb_outcome_schema import EnvelopeBuilder

CONFIG = os.path.join(os.path.dirname(__file__), "..", "utility", "connector_config_sit.yaml")
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


class TestTheEcsVariable:
    """IFC_ENVELOPE__TOKENISED_ENVIRONMENTS, as an ECS task definition would set it."""

    DEV_CONFIG = os.path.join(os.path.dirname(__file__), "..", "utility", "connector_config_dev.yaml")

    @pytest.fixture
    def load_dev(self, clean_ifc_env, monkeypatch):
        def load(value=None):
            if value is not None:
                monkeypatch.setenv("IFC_ENVELOPE__TOKENISED_ENVIRONMENTS", value)
            return load_settings(self.DEV_CONFIG)

        return load

    def test_dev_declares_none_by_default(self, load_dev):
        assert load_dev().declares_encryption_policies is False

    @pytest.mark.parametrize("value", ["DEV", "dev", "DEV,PROD", " DEV , PROD ", '["DEV","PROD"]'])
    def test_naming_dev_turns_it_on(self, load_dev, value):
        assert load_dev(value).declares_encryption_policies is True

    def test_the_variable_replaces_the_files_list(self, load_dev):
        assert load_dev("DEV").envelope.tokenised_environments == ["DEV"]

    @pytest.mark.parametrize("value", ["", "  "])
    def test_an_empty_variable_leaves_it_to_the_config_file(self, load_dev, value):
        assert load_dev(value).envelope.tokenised_environments == ["PROD"]
