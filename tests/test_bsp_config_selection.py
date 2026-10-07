"""Each environment's BSP client config: DEV runs on the PLAINTEXT DEV file, SIT on SASL_SSL."""

from __future__ import annotations

import os

import pytest
import yaml

from utility.connector_config import load_settings

UTILITY = os.path.join(os.path.dirname(__file__), "..", "utility")
CONFIG = os.path.join(UTILITY, "connector_config.yaml")


def bsp_yaml(name: str) -> dict:
    with open(os.path.join(UTILITY, name), encoding="utf-8") as handle:
        return yaml.safe_load(handle)


@pytest.fixture
def load(monkeypatch):
    def load(environment: str, **env: str):
        for name in ("IFC_KAFKA__BSP_CONFIG_PATH",):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("IFC_APP__ENVIRONMENT", environment)
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        return load_settings(CONFIG)

    return load


class TestSelection:
    @pytest.mark.parametrize("environment", ["DEV", "dev"])
    def test_a_dev_run_uses_the_dev_file(self, load, environment):
        assert load(environment).kafka.bsp_config_path == "utility/bsp_dev_config.yaml"

    def test_a_sit_run_uses_the_sit_file(self, load):
        assert load("SIT").kafka.bsp_config_path == "utility/bsp_sit_config.yaml"

    def test_an_environment_without_its_own_file_keeps_the_default(self, load):
        assert load("PROD").kafka.bsp_config_path == "utility/bsp_sit_config.yaml"

    def test_an_explicit_path_on_the_task_wins(self, load):
        settings = load("DEV", IFC_KAFKA__BSP_CONFIG_PATH="s3://bucket/own.yaml")

        assert settings.kafka.bsp_config_path == "s3://bucket/own.yaml"

    def test_an_empty_explicit_path_does_not_count(self, load):
        assert load("DEV", IFC_KAFKA__BSP_CONFIG_PATH=" ").kafka.bsp_config_path == "utility/bsp_dev_config.yaml"


class TestTheDevFile:
    def test_it_is_plaintext_on_the_dev_only_9092_listener(self):
        dev = bsp_yaml("bsp_dev_config.yaml")

        assert dev["security"]["security.protocol"] == "PLAINTEXT"
        servers = dev["servers"]["bootstrap.servers"].split(",")
        assert servers and all(server.strip().endswith(":9092") for server in servers)

    def test_it_keeps_the_producer_guarantees(self):
        producer = bsp_yaml("bsp_dev_config.yaml")["producer"]["properties"]

        assert producer["acks"] == "all"
        assert producer["enable.idempotence"] is True

    def test_it_names_no_workstation_path(self):
        assert "C:/" not in yaml.safe_dump(bsp_yaml("bsp_dev_config.yaml"))


class TestADevRunEndToEnd:
    """DEV environment and DEV registry mode together: no token anywhere."""

    def test_the_settings_a_dev_task_gets(self, load):
        settings = load("DEV", IFC_SCHEMA_REGISTRY__MODE="DEV")

        assert settings.originating_system == "SNSVC0084379"
        assert settings.kafka.bsp_config_path == "utility/bsp_dev_config.yaml"
        assert (settings.schema_registry.mode, settings.schema_registry.schema_id) == ("DEV", 1299)
