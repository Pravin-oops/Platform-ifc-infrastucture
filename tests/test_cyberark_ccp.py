"""CyberArk CCP credential retrieval: what is retried, and how failures are filed."""

from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest
import requests

from utility import cyberark_ccp_fetch as ccp
from utility import failure_catalog as catalog
from utility.connector_config import CyberArkSettings
from utility.error_classifier import PreflightError
from utility.resilience_utility import BackoffPolicy

CERTIFICATE = {
    "client_cert_pem": "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
    "client_key_pem": "-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----\n",
}


class FakeSecrets:
    def get_secret_value(self, SecretId: str) -> Dict[str, Any]:
        return {"SecretString": json.dumps(CERTIFICATE)}


class FakeSession:
    def client(self, name: str, region_name: str) -> FakeSecrets:
        return FakeSecrets()


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


def authenticator(**overrides) -> ccp.CyberArkAuthenticator:
    settings = CyberArkSettings(
        base_url="https://ccp.example",
        app_id="APP_TEST",
        safe="SAFE",
        object="OBJECT",
        client_cert_secret_id="cert-secret",
        ssl_verify=False,
        **overrides,
    )
    return ccp.CyberArkAuthenticator(settings, session=FakeSession())


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
