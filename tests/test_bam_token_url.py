"""BAM_URL points the BSP client's token request away from the UAT URL it hardcodes."""

from __future__ import annotations

import sys
import types

import pytest

from utility.auth_helper import BSPClient, bam_token_url, point_at_bam
from utility.error_classifier import PreflightError

UAT = (
    "https://bamuat-auth.client.barclayscorp.com"
    "/authn/authenticate/sso/api?appName=BarclaysStreamingPlatform&redirectURL=http://localhost"
)


class FakeAuthenticator:
    def __init__(self):
        self._token_url = UAT


@pytest.fixture
def library(monkeypatch):
    module = types.ModuleType("bsp_python_client.auth.bsp_authenticator")
    module.BSPAuthenticator = FakeAuthenticator
    monkeypatch.setitem(sys.modules, "bsp_python_client", types.ModuleType("bsp_python_client"))
    monkeypatch.setitem(sys.modules, "bsp_python_client.auth", types.ModuleType("bsp_python_client.auth"))
    monkeypatch.setitem(sys.modules, "bsp_python_client.auth.bsp_authenticator", module)


class TestBamTokenUrl:
    def test_unset_keeps_the_library_default(self):
        assert bam_token_url(UAT, None) == UAT
        assert bam_token_url(UAT, "") == UAT

    def test_a_full_url_is_used_as_is(self):
        prod = "https://bam-auth.client.barclayscorp.com/authn/authenticate/sso/api?appName=X"
        assert bam_token_url(UAT, prod) == prod

    def test_a_bare_host_keeps_the_library_path_and_app_name(self):
        assert bam_token_url(UAT, "https://bam-auth.client.barclayscorp.com/") == (
            "https://bam-auth.client.barclayscorp.com"
            "/authn/authenticate/sso/api?appName=BarclaysStreamingPlatform&redirectURL=http://localhost"
        )


class TestPointAtBam:
    def test_bam_url_overrides_the_hardcoded_uat_url(self, monkeypatch):
        monkeypatch.setenv("BAM_URL", "https://bam-auth.client.barclayscorp.com")
        authenticator = point_at_bam(FakeAuthenticator())
        assert authenticator._token_url.startswith("https://bam-auth.client.barclayscorp.com/authn/")

    def test_without_bam_url_the_default_is_kept_and_called_out(self, monkeypatch, caplog):
        monkeypatch.delenv("BAM_URL", raising=False)
        authenticator = point_at_bam(FakeAuthenticator())
        assert authenticator._token_url == UAT
        assert "BAM_URL is not set" in caplog.text

    def test_a_library_without_token_url_fails_loudly_when_bam_url_is_set(self, monkeypatch):
        monkeypatch.setenv("BAM_URL", "https://bam-auth.client.barclayscorp.com")
        with pytest.raises(PreflightError, match="_token_url"):
            point_at_bam(object())

    def test_the_bsp_client_applies_bam_url_to_the_authenticator_it_builds(self, library, monkeypatch):
        monkeypatch.setenv("BAM_URL", "https://bam-auth.client.barclayscorp.com")
        authenticator = BSPClient("unused.yaml")._auth()
        assert isinstance(authenticator, FakeAuthenticator)
        assert authenticator._token_url.startswith("https://bam-auth.client.barclayscorp.com/")

    def test_an_injected_authenticator_is_left_alone(self, monkeypatch):
        monkeypatch.setenv("BAM_URL", "https://bam-auth.client.barclayscorp.com")
        injected = FakeAuthenticator()
        assert BSPClient("unused.yaml", authenticator=injected)._auth()._token_url == UAT
