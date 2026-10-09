"""Each environment's BSP client config: DEV runs on the PLAINTEXT DEV file, SIT on SASL_SSL."""

from __future__ import annotations

import os

import pytest
import yaml

from utility.connector_config import ENVIRONMENT_CONFIGS, config_path_for, load_settings

UTILITY = os.path.join(os.path.dirname(__file__), "..", "utility")


def bsp_yaml(name: str) -> dict:
    with open(os.path.join(UTILITY, name), encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def with_bsp_config_paths() -> str:
    """The SIT config with a kafka.bsp_config_paths map, which no shipped file carries."""
    document = bsp_yaml("connector_config_sit.yaml")
    document["kafka"]["bsp_config_paths"] = {
        "DEV": "utility/bsp_dev_config.yaml",
        "SIT": "utility/bsp_sit_config.yaml",
    }
    return yaml.safe_dump(document)


@pytest.fixture
def load(monkeypatch):
    def load(environment: str, **env: str):
        for name in ("IFC_KAFKA__BSP_CONFIG_PATH",):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("IFC_APP__ENVIRONMENT", environment)
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        return load_settings("cfg.yaml", reader=lambda _p: with_bsp_config_paths())

    return load


class TestSelection:
    """kafka.bsp_config_paths, for a config that maps BSP files by app.environment."""

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


class TestPerEnvironmentFiles:
    """connector_config_<env>.yaml, chosen by IFC_APP__ENVIRONMENT: each fixes its own BSP file and registry."""

    @pytest.fixture
    def load_file(self, clean_ifc_env):
        return lambda name: load_settings(os.path.join(UTILITY, name))

    def test_the_sit_file(self, load_file):
        settings = load_file("connector_config_sit.yaml")

        assert settings.app.environment == "SIT"
        assert settings.originating_system == "SNSVC0084378"
        assert settings.kafka.bsp_config_path == "utility/bsp_sit_config.yaml"
        assert settings.schema_registry.mode == "SECURE"
        assert all(url.endswith(":8095") for url in settings.schema_registry.urls)

    def test_the_dev_file(self, load_file):
        settings = load_file("connector_config_dev.yaml")

        assert settings.app.environment == "DEV"
        assert settings.originating_system == "SNSVC0084379"
        assert settings.kafka.bsp_config_path == "utility/bsp_dev_config.yaml"
        assert (settings.schema_registry.mode, settings.schema_registry.schema_id) == ("DEV", 1299)

    @pytest.mark.parametrize("name", ["connector_config_sit.yaml", "connector_config_dev.yaml"])
    def test_neither_maps_bsp_files_by_environment(self, name):
        with open(os.path.join(UTILITY, name), encoding="utf-8") as handle:
            assert "bsp_config_paths" not in yaml.safe_load(handle)["kafka"]


class TestEnvironmentSelectsTheConfig:
    """IFC_APP__ENVIRONMENT names the environment; the connector config, and through it the BSP file, follow."""

    @pytest.mark.parametrize("environment, name", [("DEV", "dev"), ("sit", "sit"), (" Sit ", "sit")])
    def test_each_environment_has_its_own_file(self, environment, name):
        path = config_path_for(environment)
        assert os.path.normpath(path).endswith(os.path.join("utility", f"connector_config_{name}.yaml"))

    @pytest.mark.parametrize("environment", sorted(ENVIRONMENT_CONFIGS))
    def test_each_connector_config_names_its_own_bsp_file(self, environment, clean_ifc_env):
        if not os.path.isfile(os.path.join(UTILITY, "..", ENVIRONMENT_CONFIGS[environment])):
            pytest.skip(f"{ENVIRONMENT_CONFIGS[environment]} is not in the repository yet")
        settings = load_settings(config_path_for(environment))

        assert settings.app.environment == environment
        assert settings.kafka.bsp_config_path == f"utility/bsp_{environment.lower()}_config.yaml"

    @pytest.mark.parametrize("environment", [None, "", "  "])
    def test_no_environment_is_refused(self, environment):
        with pytest.raises(ValueError, match="IFC_APP__ENVIRONMENT is not set"):
            config_path_for(environment)

    @pytest.mark.parametrize("environment", ["UAT", "PROD-ANALYTICS", "LOCAL"])
    def test_an_unknown_environment_is_refused(self, environment):
        with pytest.raises(ValueError, match="expected one of DEV, SIT, PROD"):
            config_path_for(environment)

    def test_a_file_missing_from_the_image_is_refused(self, monkeypatch, tmp_path):
        monkeypatch.setenv("IFC_HOME", str(tmp_path))
        with pytest.raises(FileNotFoundError, match="connector_config_sit.yaml"):
            config_path_for("SIT")
