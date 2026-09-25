"""Resolution of the bundled configs and schemas after the flattening.

Moving ``schemas/avro/*.avsc`` into ``utility/*.json`` changed two things at
once: the names the config files point at, and the depth ``package_resource``
has to walk up from to find the app root. Either one being wrong produces a
container that passes every unit test and then cannot find its own schema on
startup, so both are pinned here.
"""

from __future__ import annotations

import json
import os

import pytest
import yaml
from fastavro import parse_schema

from ifc_trigger_connector.utility.connector_config import (
    ConnectorSettings,
    SchemaRegistrySettings,
)
from ifc_trigger_connector.utility.connector_utility import (
    load_schema_document,
    package_resource,
)
from tests.conftest import APP_ROOT, SAMPLES_DIR, UTILITY_DIR

AVRO_SCHEMAS = ["utility/schema.json"]

CONFIGS = ["connector_config.yaml", "connector_config_local.yaml"]


class TestPackageResource:
    def test_a_relative_path_resolves_against_the_app_root(self):
        resolved = package_resource("utility/schema.json")

        assert os.path.isfile(resolved)
        assert os.path.dirname(os.path.dirname(resolved)) == APP_ROOT

    def test_ifc_home_wins_when_set(self, monkeypatch, tmp_path):
        """This is the Dockerfile's mechanism; it has to keep working."""
        monkeypatch.setenv("IFC_HOME", str(tmp_path))
        assert package_resource("utility/schema.json") == os.path.join(
            str(tmp_path), "utility/schema.json"
        )

    def test_an_absolute_path_is_passed_through(self, monkeypatch, tmp_path):
        monkeypatch.setenv("IFC_HOME", str(tmp_path))
        absolute = os.path.join(str(tmp_path), "elsewhere.json")
        assert package_resource(absolute) == absolute

    def test_an_s3_uri_is_passed_through(self):
        uri = "s3://bucket/triggerbackbone/ifc/config/schema.json"
        assert package_resource(uri) == uri


class TestBundledSchemas:
    @pytest.mark.parametrize("relative", AVRO_SCHEMAS)
    def test_avro_schema_loads_and_parses(self, relative):
        schema = load_schema_document(relative)

        assert schema["type"] == "record"
        assert schema["name"]
        assert schema["fields"]
        # parse_schema is what the serializer will do at startup.
        assert parse_schema(schema) is not None

    def test_the_topic_schema_carries_the_envelope_fields(self):
        schema = load_schema_document("utility/schema.json")
        names = {field["name"] for field in schema["fields"]}

        assert {
            "triggerID",
            "triggerType",
            "triggerSubType",
            "timestamp",
            "sequenceNumber",
            "triggerOriginatingSystem",
            "triggerOriginatingBU",
            "idType",
            "idValue",
            "idSystem",
            "payload",
        } <= names


class TestSchemaDefaults:
    def test_the_default_schema_path_points_at_a_bundled_file(self):
        settings = SchemaRegistrySettings(mode="DEV")

        assert settings.schema_path == "utility/schema.json"
        assert os.path.isfile(package_resource(settings.schema_path))


class TestBundledConfigs:
    @pytest.mark.parametrize("name", CONFIGS)
    def test_config_is_valid_yaml(self, name):
        with open(os.path.join(UTILITY_DIR, name), "r", encoding="utf-8") as handle:
            assert isinstance(yaml.safe_load(handle), dict)

    @pytest.mark.parametrize("name", CONFIGS)
    def test_config_validates_against_the_settings_model(self, name, clean_ifc_env):
        with open(os.path.join(UTILITY_DIR, name), "r", encoding="utf-8") as handle:
            document = yaml.safe_load(handle)

        settings = ConnectorSettings.model_validate(document)

        assert settings.kafka.topic
        assert settings.app.name

    @pytest.mark.parametrize("name", CONFIGS)
    def test_config_points_at_schemas_that_exist(self, name, clean_ifc_env):
        with open(os.path.join(UTILITY_DIR, name), "r", encoding="utf-8") as handle:
            settings = ConnectorSettings.model_validate(yaml.safe_load(handle))

        assert os.path.isfile(package_resource(settings.schema_registry.schema_path))

    def test_the_local_config_needs_no_aws_or_bsp(self, clean_ifc_env):
        path = os.path.join(UTILITY_DIR, "connector_config_local.yaml")
        with open(path, "r", encoding="utf-8") as handle:
            settings = ConnectorSettings.model_validate(yaml.safe_load(handle))

        assert settings.source.type == "local"
        assert settings.schema_registry.mode == "DEV"
        assert settings.kafka.bsp_config_path is None
        assert settings.csm is None

    def test_the_local_config_points_at_the_bundled_samples(self, clean_ifc_env):
        path = os.path.join(UTILITY_DIR, "connector_config_local.yaml")
        with open(path, "r", encoding="utf-8") as handle:
            settings = ConnectorSettings.model_validate(yaml.safe_load(handle))

        # Relative to the connector directory, which is where a local run starts.
        assert os.path.isdir(os.path.join(APP_ROOT, settings.source.path))

    def test_the_uat_config_keeps_the_secure_posture(self, clean_ifc_env):
        path = os.path.join(UTILITY_DIR, "connector_config.yaml")
        with open(path, "r", encoding="utf-8") as handle:
            settings = ConnectorSettings.model_validate(yaml.safe_load(handle))

        assert settings.schema_registry.mode == "SECURE"
        assert settings.schema_registry.url
        assert settings.kafka.bsp_config_path
        assert settings.csm is not None

    def test_the_ecs_local_config_points_at_the_bundled_bsp_config(self, clean_ifc_env):
        path = os.path.join(UTILITY_DIR, "connector_config_ecs_local.yaml")
        with open(path, "r", encoding="utf-8") as handle:
            settings = ConnectorSettings.model_validate(yaml.safe_load(handle))

        assert settings.kafka.bsp_config_path == "utility/bsp_config_local.yaml"
        assert os.path.isfile(package_resource(settings.kafka.bsp_config_path))

    def test_a_relative_bsp_config_path_does_not_depend_on_the_working_directory(
        self, clean_ifc_env, monkeypatch, tmp_path
    ):
        from ifc_trigger_connector.utility import kafka_factory
        from ifc_trigger_connector.utility.resilience_utility import ShutdownSignal

        path = os.path.join(UTILITY_DIR, "connector_config_ecs_local.yaml")
        with open(path, "r", encoding="utf-8") as handle:
            document = yaml.safe_load(handle)
        document.pop("csm")  # no CSM fetch in a unit test
        settings = ConnectorSettings.model_validate(document)

        captured = {}
        monkeypatch.setattr(kafka_factory, "BSPClient", lambda config_path: captured.setdefault("path", config_path))
        monkeypatch.delenv("IFC_HOME", raising=False)
        monkeypatch.chdir(tmp_path)

        kafka_factory.KafkaStackFactory(settings, metrics=None, shutdown=ShutdownSignal())._bsp_client()

        assert os.path.isfile(captured["path"])
        assert os.path.samefile(captured["path"], os.path.join(APP_ROOT, "utility", "bsp_config_local.yaml"))

    def test_no_config_carries_a_secret(self):
        """CSM supplies credentials at runtime; only locations are configuration."""
        for name in CONFIGS:
            text = open(os.path.join(UTILITY_DIR, name), "r", encoding="utf-8").read().lower()
            for smell in ("password:", "sasl.password", "secret_key", "private_key"):
                assert smell not in text, f"{name} looks like it contains a credential"


class TestSamples:
    def test_the_sample_file_is_where_the_local_config_expects_it(self):
        assert os.path.isfile(os.path.join(SAMPLES_DIR, "trigger_events.jsonl"))

    def test_every_sample_line_is_a_json_object(self, samples_jsonl):
        with open(samples_jsonl, "r", encoding="utf-8") as handle:
            lines = [line for line in handle.read().splitlines() if line.strip()]

        assert lines
        for line in lines:
            assert isinstance(json.loads(line), dict)

    def test_the_expected_message_fixture_survived_the_move(self):
        path = os.path.join(SAMPLES_DIR, "expected", "trigger_8_message.json")
        assert os.path.isfile(path)

        with open(path, "r", encoding="utf-8") as handle:
            assert isinstance(json.load(handle), dict)
