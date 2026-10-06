"""Barclays root CA from Secrets Manager: written where the BSP YAML and the registry read it."""

from __future__ import annotations

import os
from typing import Any, Dict

import pytest
import yaml
from botocore.exceptions import ClientError

from utility import ca_certificate
from utility import failure_catalog as catalog
from utility.connector_config import CaCertificateSettings, load_settings
from utility.error_classifier import PreflightError

CA_PEM = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"


class FakeSecrets:
    def __init__(self, response: Any):
        self._response = response

    def get_secret_value(self, SecretId: str) -> Dict[str, Any]:
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


class FakeSession:
    def __init__(self, response: Any):
        self._response = response

    def client(self, name: str, region_name: str) -> FakeSecrets:
        return FakeSecrets(self._response)


def settings(tmp_path, **overrides) -> CaCertificateSettings:
    fields: Dict[str, Any] = {"secret_id": "ca-secret", "path": str(tmp_path / "certs" / "CARoot.pem")}
    return CaCertificateSettings(**{**fields, **overrides})


def test_the_ca_is_written_to_its_path(tmp_path):
    config = settings(tmp_path)

    path = ca_certificate.install(config, session=FakeSession({"SecretString": CA_PEM}))

    assert path == config.path
    with open(path, encoding="utf-8") as handle:
        assert handle.read() == CA_PEM + "\n"
    # Only the target is left behind, not the staging file.
    assert os.listdir(os.path.dirname(path)) == ["CARoot.pem"]


def test_a_binary_secret_is_accepted(tmp_path):
    config = settings(tmp_path)

    ca_certificate.install(config, session=FakeSession({"SecretBinary": CA_PEM.encode()}))

    assert os.path.exists(config.path)


def test_no_secret_id_writes_nothing(tmp_path):
    config = settings(tmp_path, secret_id=None)

    assert ca_certificate.install(config, session=FakeSession({"SecretString": CA_PEM})) is None
    assert not os.path.exists(config.path)


@pytest.mark.parametrize(
    "response, message",
    [
        ({"SecretString": ""}, "empty or not a PEM"),
        ({"SecretString": "not a certificate"}, "empty or not a PEM"),
        (
            {"SecretString": CA_PEM + "\n-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----"},
            "contains a private key",
        ),
    ],
)
def test_an_unusable_secret_fails_before_anything_is_written(tmp_path, response, message):
    config = settings(tmp_path)

    with pytest.raises(PreflightError, match=message) as exc:
        ca_certificate.install(config, session=FakeSession(response))

    assert exc.value.scenario is catalog.CONTAINER_FAILURE
    assert not os.path.exists(config.path)


def test_an_unreadable_secret_names_the_aws_error(tmp_path):
    denied = ClientError({"Error": {"Code": "AccessDeniedException"}}, "GetSecretValue")

    with pytest.raises(PreflightError, match="AccessDeniedException"):
        ca_certificate.install(settings(tmp_path), session=FakeSession(denied))


def test_the_bsp_yaml_and_the_registry_read_the_file_the_ca_is_written_to():
    """The three paths must agree, or the CA is written where nobody reads it."""
    utility = os.path.join(os.path.dirname(__file__), "..", "utility")
    connector = load_settings(os.path.join(utility, "connector_config.yaml"))
    with open(os.path.join(utility, "bsp_sit_config.yaml"), encoding="utf-8") as handle:
        bsp = yaml.safe_load(handle)

    assert connector.ca_certificate.secret_id
    assert connector.schema_registry.ca_location == connector.ca_certificate.path
    assert bsp["security"]["ssl.ca.location"] == connector.ca_certificate.path
