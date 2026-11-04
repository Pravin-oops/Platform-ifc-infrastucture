"""BAM_URL / BAM_NAME: the PROD values are the default, and the environment overrides them."""

from __future__ import annotations

import logging
import os

import pytest
import yaml

from utility.auth_helper import export_bam_environment
from utility.connector_config import PROD_BAM_NAME, PROD_BAM_URL, BamSettings, load_settings
from utility.error_classifier import PreflightError

UTILITY = os.path.join(os.path.dirname(__file__), "..", "utility")
SIT_URL = "https://bamuat-auth.client.barclayscorp.com/authn/authenticate/sso/api"


@pytest.fixture(autouse=True)
def no_bam_environment(monkeypatch):
    # Empty means unset; setenv also makes monkeypatch undo what export_bam_environment writes.
    monkeypatch.setenv("BAM_URL", "")
    monkeypatch.setenv("BAM_NAME", "")


def load(monkeypatch, **env: str):
    monkeypatch.setenv("IFC_APP__ENVIRONMENT", "SIT")
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    with open(os.path.join(UTILITY, "connector_config_sit.yaml"), encoding="utf-8") as handle:
        document = handle.read()
    return load_settings("cfg.yaml", reader=lambda _p: document)


class TestBamSettings:
    def test_without_environment_the_prod_defaults_apply(self, monkeypatch):
        bam = load(monkeypatch).bam
        assert (bam.url, bam.name) == (PROD_BAM_URL, PROD_BAM_NAME)
        assert bam.placeholders == ["BAM_URL", "BAM_NAME"]

    def test_the_environment_overrides_both(self, monkeypatch):
        bam = load(monkeypatch, BAM_URL=f" {SIT_URL}/ ", BAM_NAME="SIT_APP").bam
        assert (bam.url, bam.name) == (SIT_URL, "SIT_APP")
        assert bam.placeholders == []

    def test_one_override_keeps_the_other_prod_default(self, monkeypatch):
        bam = load(monkeypatch, BAM_NAME="SIT_APP").bam
        assert (bam.url, bam.name) == (PROD_BAM_URL, "SIT_APP")
        assert bam.placeholders == ["BAM_URL"]

    def test_a_query_string_in_bam_url_is_rejected(self, monkeypatch):
        with pytest.raises(ValueError, match="must not contain a query string"):
            load(monkeypatch, BAM_URL=f"{SIT_URL}?appName=X")


class TestExportBamEnvironment:
    def test_an_unfilled_prod_placeholder_fails_before_bam_is_called(self):
        with pytest.raises(PreflightError, match="BAM_URL and BAM_NAME not set"):
            export_bam_environment(BamSettings())

    def test_the_resolved_values_are_exported_for_the_bsp_client(self, monkeypatch, caplog):
        caplog.set_level(logging.INFO)
        monkeypatch.setenv("BAM_NAME", "SIT_APP")
        export_bam_environment(BamSettings(url="https://bam-prod.example/sso/api", name="SIT_APP"))

        assert os.environ["BAM_URL"] == "https://bam-prod.example/sso/api"
        assert os.environ["BAM_NAME"] == "SIT_APP"
        assert "BAM_URL from PROD default, BAM_NAME from environment" in caplog.text

    def test_the_shipped_bsp_configs_read_bam_from_the_environment(self):
        for name in ("bsp_dev_config.yaml", "bsp_sit_config.yaml"):
            with open(os.path.join(UTILITY, name), encoding="utf-8") as handle:
                bam = yaml.safe_load(handle)["bam"]
            assert bam == {"method": "env", "bam_url": "BAM_URL", "bam_name": "BAM_NAME"}, name
