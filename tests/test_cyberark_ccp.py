"""CyberArk CCP credential retrieval: what is retried, and how failures are filed."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

import pytest
import requests

from utility import cyberark_ccp_fetch as ccp
from utility import failure_catalog as catalog
from utility.connector_config import CyberArkSettings, load_settings
from utility.error_classifier import PreflightError
from utility.resilience_utility import BackoffPolicy

SECRETS = {
    "cert-secret": "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
    "key-secret": "-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----\n",
}


class FakeSecrets:
    def __init__(self, values: Dict[str, str]):
        self._values = values

    def get_secret_value(self, SecretId: str) -> Dict[str, Any]:
        return {"SecretString": self._values[SecretId]}


class FakeSession:
    def __init__(self, values: Dict[str, str] = SECRETS):
        self._values = values

    def client(self, name: str, region_name: str) -> FakeSecrets:
        return FakeSecrets(self._values)


class FakeResponse:
    def __init__(self, status_code: int, body: Dict[str, Any]):
        self.status_code = status_code
        self._body = body
        self.text = json.dumps(body)

    def json(self) -> Dict[str, Any]:
        return self._body


ACCOUNT = FakeResponse(200, {"UserName": "svcaccount", "Content": "secret"})


@pytest.fixture
def ccp_responses(monkeypatch):
    """Serve the queued responses in order, and count the CCP calls."""
    queue: List[FakeResponse] = []
    calls = {"n": 0}

    def get(*_args, **_kwargs):
        calls["n"] += 1
        return queue.pop(0)

    monkeypatch.setattr(ccp.requests, "get", get)
    monkeypatch.setattr(ccp, "_BACKOFF", BackoffPolicy(base_seconds=0.001, max_seconds=0.001))
    return queue, calls


def authenticator(secrets: Dict[str, str] = SECRETS, **overrides) -> ccp.CyberArkAuthenticator:
    fields: Dict[str, Any] = {
        "base_url": "https://ccp.example",
        "app_id": "APP_TEST",
        "safe": "SAFE",
        "object": "sysaccount",
        "client_cert_secret_id": "cert-secret",
        "client_key_secret_id": "key-secret",
        "ssl_verify": False,
        **overrides,
    }
    settings = CyberArkSettings(**fields)
    return ccp.CyberArkAuthenticator(settings, session=FakeSession(secrets))


class TestRetry:
    @pytest.mark.parametrize("status", [500, 503, 429])
    def test_an_unavailable_ccp_is_retried(self, ccp_responses, status):
        queue, calls = ccp_responses
        queue.extend([FakeResponse(status, {"ErrorCode": "APPAP282E"}), ACCOUNT])

        credentials = authenticator().get_credentials()

        assert credentials.username == "svcaccount"
        assert calls["n"] == 2

    def test_a_ccp_that_stays_down_is_a_network_failure(self, ccp_responses):
        queue, calls = ccp_responses
        queue.extend([FakeResponse(503, {}) for _ in range(3)])

        with pytest.raises(PreflightError) as exc:
            authenticator(max_attempts=3).get_credentials()

        assert exc.value.scenario is catalog.NETWORK_FAILURE
        assert calls["n"] == 3

    def test_a_connection_failure_is_retried(self, monkeypatch, ccp_responses):
        _, calls = ccp_responses
        responses = iter([requests.ConnectionError("refused"), ACCOUNT])

        def get(*_args, **_kwargs):
            calls["n"] += 1
            outcome = next(responses)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        monkeypatch.setattr(ccp.requests, "get", get)

        assert authenticator().get_credentials().username == "svcaccount"
        assert calls["n"] == 2


class TestRejection:
    @pytest.mark.parametrize("status", [403, 404])
    def test_a_rejection_fails_at_once_as_authentication(self, ccp_responses, status):
        queue, calls = ccp_responses
        queue.extend([FakeResponse(status, {"ErrorCode": "APPAP004E"}), ACCOUNT])

        with pytest.raises(PreflightError) as exc:
            authenticator().get_credentials()

        assert exc.value.scenario is catalog.AUTHENTICATION_FAILURE
        assert calls["n"] == 1


class TestRequestShape:
    @pytest.mark.parametrize(
        "base_url",
        [
            "https://ccp.example",
            "https://ccp.example/",
            "https://ccp.example/AIMWebService_certs/api/Accounts",
            "https://ccp.example/AIMWebService_certs/api/Accounts/",
        ],
    )
    def test_the_endpoint_path_is_added_only_once(self, base_url):
        assert authenticator(base_url=base_url).accounts_url == (
            "https://ccp.example/AIMWebService_certs/api/Accounts"
        )

    def test_the_account_is_sent_as_object(self, monkeypatch, ccp_responses):
        seen: Dict[str, Any] = {}

        def get(url, params, **_kwargs):
            seen.update(params)
            return ACCOUNT

        monkeypatch.setattr(ccp.requests, "get", get)
        authenticator().get_credentials()

        assert seen == {"AppID": "APP_TEST", "Safe": "SAFE", "Folder": "Root", "Object": "sysaccount"}


class TestClientCertificate:
    def test_the_certificate_and_key_come_from_their_own_secrets(self):
        certificate = authenticator()._load_client_certificate()

        assert "BEGIN CERTIFICATE" in certificate.cert_pem
        assert "BEGIN PRIVATE KEY" in certificate.key_pem

    @pytest.mark.parametrize(
        "secrets, message",
        [
            ({**SECRETS, "cert-secret": ""}, "client certificate secret is empty"),
            ({**SECRETS, "key-secret": "  \n"}, "client private key secret is empty"),
            ({**SECRETS, "cert-secret": "not a pem"}, "client certificate secret is not a PEM"),
            ({**SECRETS, "key-secret": SECRETS["cert-secret"]}, "client private key secret is not a PEM"),
            (
                {**SECRETS, "key-secret": "-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIE\n-----END ENCRYPTED PRIVATE KEY-----"},
                "passphrase-protected",
            ),
        ],
    )
    def test_an_unusable_secret_fails_as_authentication(self, ccp_responses, secrets, message):
        _, calls = ccp_responses

        with pytest.raises(PreflightError, match=message) as exc:
            authenticator(secrets).get_credentials()

        assert exc.value.scenario is catalog.AUTHENTICATION_FAILURE
        assert calls["n"] == 0


class TestSettings:
    SECRETS = {"client_cert_secret_id": "c", "client_key_secret_id": "k"}

    def test_an_enabled_section_needs_the_whole_query(self):
        with pytest.raises(ValueError) as exc:
            CyberArkSettings(base_url="https://ccp.example", **self.SECRETS)

        for missing in ("CYBERARK_APP_ID", "CYBERARK_SAFE", "CYBERARK_ACCOUNT"):
            assert missing in str(exc.value)
        assert "CYBERARK_CCP_URL" not in str(exc.value)

    def test_a_disabled_section_needs_no_query(self):
        assert CyberArkSettings(enabled=False, **self.SECRETS).enabled is False


class TestTemplateEnvironment:
    """The ECS product template's CYBERARK_* variables fill the cyberark section."""

    TEMPLATE = {
        "CYBERARK_ENABLED": "true",
        "CYBERARK_CCP_URL": "https://ccp.example/AIMWebService_certs/api/Accounts",
        "CYBERARK_APP_ID": "APP_TEST",
        "CYBERARK_SAFE": "SAFE",
        "CYBERARK_ACCOUNT": "sysaccount",
    }

    @pytest.fixture
    def load(self, monkeypatch):
        for name in list(os.environ):
            if name.startswith(("CYBERARK_", "IFC_")):
                monkeypatch.delenv(name)
        path = os.path.join(os.path.dirname(__file__), "..", "utility", "connector_config_sit.yaml")

        def load(**env: str):
            for name, value in env.items():
                monkeypatch.setenv(name, value)
            return load_settings(path).cyberark

        return load

    def test_the_template_variables_fill_the_query(self, load):
        cyberark = load(**self.TEMPLATE)

        assert cyberark.enabled is True
        assert cyberark.base_url == self.TEMPLATE["CYBERARK_CCP_URL"]
        assert (cyberark.app_id, cyberark.safe, cyberark.object) == ("APP_TEST", "SAFE", "sysaccount")

    def test_the_yaml_alone_leaves_cyberark_off(self, load):
        assert load().enabled is False

    def test_the_template_default_of_false_turns_it_off(self, load):
        assert load(**{**self.TEMPLATE, "CYBERARK_ENABLED": "false"}).enabled is False

    def test_an_ifc_variable_still_overrides_the_template(self, load):
        cyberark = load(**self.TEMPLATE, IFC_CYBERARK__OBJECT="other")

        assert cyberark.object == "other"
