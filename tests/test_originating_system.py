"""triggerOriginatingSystem and idSystem follow app.environment."""

from __future__ import annotations

import os

import pytest

from utility.connector_config import load_settings
from utility.connector_utility import load_schema_document
from tests.test_envelope import trigger_8_event
from utility.tb_outcome_schema import EnvelopeBuilder

CONFIG = os.path.join(os.path.dirname(__file__), "..", "utility", "connector_config.yaml")

#: The service number agreed for each environment.
EXPECTED = {
    "DEV": "SNSVC0084379",
    "SIT": "SNSVC0084378",
    "PROD-ANALYTICS": "SNSVC0084375",
    "PROD-PARALLEL": "SNSVC0084371",
    "PROD": "SNSVC0084373",
}


@pytest.fixture
def settings_for(monkeypatch):
    def load(environment: str):
        monkeypatch.setenv("IFC_APP__ENVIRONMENT", environment)
        return load_settings(CONFIG)

    return load


@pytest.mark.parametrize("environment, code", sorted(EXPECTED.items()))
def test_each_environment_has_its_own_code(settings_for, environment, code):
    assert settings_for(environment).originating_system == code


def test_the_environment_name_matches_in_any_case(settings_for):
    assert settings_for("prod-analytics").originating_system == "SNSVC0084375"


def test_an_unknown_environment_stops_rather_than_borrowing_a_code(settings_for):
    settings = settings_for("UAT")

    with pytest.raises(ValueError, match="'UAT' has no envelope.originating_systems entry"):
        settings.originating_system


@pytest.mark.parametrize("environment, code", sorted(EXPECTED.items()))
def test_the_record_carries_the_code_as_both_fields_and_starts_its_trigger_id(environment, code):
    builder = EnvelopeBuilder(
        avro_schema=load_schema_document("utility/schema.json"),
        originating_system=code,
        business_month="2026-09",
    )
    built = builder.build(trigger_8_event())

    assert built.record["triggerOriginatingSystem"] == code
    assert built.record["idSystem"] == code
    assert built.trigger_id.startswith(f"{code}_KYCRefresh_")
    assert '"system":"%s"' % code in built.business_key


def test_a_builder_without_a_code_is_refused():
    with pytest.raises(ValueError, match="originating system"):
        EnvelopeBuilder(avro_schema=load_schema_document("utility/schema.json"), originating_system=" ")
