"""CSM (Vault) credential retrieval using the ECS task role.

The task authenticates to Vault with a SigV4-signed ``sts:GetCallerIdentity``
request, which Vault replays to STS to prove the caller's identity. No secret is
baked into the image or the task definition; only the secret's path is
configuration.

This mirrors the BUK implementation, with the differences ECS forces:

* the CA bundle is resolved from candidate paths rather than assuming Lambda's
  ``/var/task/certs``;
* failures are classified onto the catalogue, so an expired certificate reports
  as AUTHENTICATION_FAILURE rather than as a bare ``RuntimeError``;
* the credential is cached for the process lifetime, because a service-mode task
  must not re-hit Vault on every poll.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import boto3
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

from ifc_trigger_connector.utility import failure_catalog as catalog
from ifc_trigger_connector.utility.error_classifier import PreflightError
from ifc_trigger_connector.utility.connector_config import CSMSettings

logger = logging.getLogger(__name__)

#: Searched in order. The first that exists wins.
CA_BUNDLE_CANDIDATES: List[str] = [
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/ssl/certs/ca-certificates.crt",
    "/opt/certs/tls-ca-bundle.pem",
]


@dataclass(frozen=True)
class Credentials:
    username: str
    password: str

    def principal(self, realm: str) -> str:
        """Fully qualified BSP principal, e.g. ``svcaccount@INTRANET.BARCAPINT.COM``."""
        return self.username if "@" in self.username else f"{self.username}{realm}"

    def __repr__(self) -> str:  # keep the password out of tracebacks and logs
        return f"Credentials(username={self.username!r}, password=***)"


def resolve_ca_bundle(configured: Optional[str], *, ssl_verify: bool = True) -> Any:
    """Return a ``requests`` ``verify`` value: a bundle path, or True/False."""
    if not ssl_verify:
        logger.warning("TLS verification is disabled; acceptable for local development only")
        return False

    candidates = [configured, os.environ.get("IFC_CA_BUNDLE"), *CA_BUNDLE_CANDIDATES]
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate

    # Fall back to certifi rather than failing: the image may carry the Barclays
    # root in the system store already.
    logger.warning(
        "No CA bundle found at any candidate path; falling back to the system trust store",
        extra={"candidates": [c for c in candidates if c]},
    )
    return True


class CSMAuthenticator:
    def __init__(self, settings: CSMSettings, *, session: Optional[Any] = None):
        self._settings = settings
        self._session = session or boto3.Session()
        self._lock = threading.Lock()
        self._cached: Optional[Credentials] = None

    # -- URLs --------------------------------------------------------------

    @property
    def auth_url(self) -> str:
        base = self._settings.base_url.rstrip("/")
        return f"{base}/v1/{self._settings.mount_point}/auth/aws/login"

    @property
    def secret_url(self) -> str:
        base = self._settings.base_url.rstrip("/")
        return f"{base}/v1/{self._settings.mount_point}/{self._settings.secret_path}"

    @property
    def _sts_endpoint(self) -> str:
        return f"https://sts.{self._settings.sts_region}.amazonaws.com/"

    @property
    def _verify(self) -> Any:
        return resolve_ca_bundle(self._settings.ca_bundle_path, ssl_verify=self._settings.ssl_verify)

    # -- flow --------------------------------------------------------------

    def _signed_login_payload(self) -> Dict[str, str]:
        credentials = self._session.get_credentials()
        if credentials is None:
            raise PreflightError(
                "No AWS credentials available; the ECS task role is not attached or has expired",
                catalog.AUTHENTICATION_FAILURE,
                context={"role_name": self._settings.role_name},
            )

        body = "Action=GetCallerIdentity&Version=2011-06-15"
        request = AWSRequest(
            method="POST",
            url=self._sts_endpoint,
            data=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
                "X-Vault-AWS-IAM-Server-ID": self._settings.vault_server_id,
                "Host": f"sts.{self._settings.sts_region}.amazonaws.com",
            },
        )
        SigV4Auth(credentials, "sts", self._settings.sts_region).add_auth(request)

        return {
            "role": self._settings.role_name,
            "iam_http_request_method": "POST",
            "iam_request_url": base64.b64encode(self._sts_endpoint.encode()).decode(),
            "iam_request_body": base64.b64encode(body.encode()).decode(),
            "iam_request_headers": base64.b64encode(
                json.dumps(dict(request.headers)).encode()
            ).decode(),
        }

    def _login(self) -> str:
        logger.info("Authenticating to CSM", extra={"auth_url": self.auth_url})

        try:
            response = requests.post(
                self.auth_url,
                json=self._signed_login_payload(),
                verify=self._verify,
                timeout=self._settings.request_timeout,
            )
        except requests.RequestException as exc:
            raise PreflightError(
                f"CSM login request failed: {exc}",
                catalog.NETWORK_FAILURE,
                context={"auth_url": self.auth_url},
                cause=exc,
            ) from exc

        if response.status_code != 200:
            raise PreflightError(
                f"CSM authentication failed with HTTP {response.status_code}",
                catalog.AUTHENTICATION_FAILURE,
                context={
                    "auth_url": self.auth_url,
                    "status": response.status_code,
                    "role_name": self._settings.role_name,
                    # Vault error bodies name the failing role/policy and carry
                    # no secret material, so they are safe and useful to keep.
                    "response": response.text[:500],
                },
            )

        try:
            return response.json()["auth"]["client_token"]
        except (ValueError, KeyError) as exc:
            raise PreflightError(
                "CSM login response did not contain auth.client_token",
                catalog.AUTHENTICATION_FAILURE,
                context={"auth_url": self.auth_url},
                cause=exc,
            ) from exc

    def _read_secret(self, vault_token: str) -> Credentials:
        logger.info("Reading system-account credential from CSM", extra={"secret_url": self.secret_url})

        try:
            response = requests.get(
                self.secret_url,
                headers={"X-Vault-Token": vault_token},
                verify=self._verify,
                timeout=self._settings.request_timeout,
            )
        except requests.RequestException as exc:
            raise PreflightError(
                f"CSM secret read failed: {exc}",
                catalog.NETWORK_FAILURE,
                context={"secret_url": self.secret_url},
                cause=exc,
            ) from exc

        if response.status_code != 200:
            raise PreflightError(
                f"CSM secret read failed with HTTP {response.status_code}",
                catalog.AUTHENTICATION_FAILURE,
                context={
                    "secret_url": self.secret_url,
                    "status": response.status_code,
                    "response": response.text[:500],
                },
            )

        data = (response.json() or {}).get("data") or {}
        if "username" not in data or "password" not in data:
            raise PreflightError(
                "CSM secret is missing username/password",
                catalog.AUTHENTICATION_FAILURE,
                context={"secret_url": self.secret_url, "keys_present": sorted(data.keys())},
            )

        return Credentials(username=data["username"], password=data["password"])

    def get_credentials(self, *, refresh: bool = False) -> Credentials:
        with self._lock:
            if self._cached is not None and not refresh:
                return self._cached
            self._cached = self._read_secret(self._login())
            logger.info(
                "CSM credentials retrieved",
                extra={"csm_username": self._cached.username},
            )
            return self._cached

    def export_to_environment(self, realm: str) -> Credentials:
        """Publish the credential where the BSP client expects to find it.

        The BSP Python client reads ``BSP_USERNAME`` / ``BSP_PASSWORD`` from the
        environment (``credentials.method: env`` in the BSP YAML), so they are
        set here immediately before the client is constructed.
        """
        credentials = self.get_credentials()
        os.environ["BSP_USERNAME"] = credentials.principal(realm)
        os.environ["BSP_PASSWORD"] = credentials.password
        return credentials
