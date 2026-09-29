"""CyberArk Central Credential Provider (CCP) credential retrieval.

The task proves its identity to CCP with a bank-signed client certificate; CCP
checks the certificate's serial number against the restriction registered on
the Application ID and returns the account from the Safe. The certificate and
its private key live in AWS Secrets Manager and are read with the ECS task
role, so nothing secret is baked into the image or the task definition; only
the secret's *name* and the CCP query are configuration.

The Secrets Manager secret is a JSON object::

    {"client_cert_pem": "-----BEGIN CERTIFICATE-----...",
     "client_key_pem":  "-----BEGIN PRIVATE KEY-----..."}

The key must be an unencrypted PEM: ``requests`` has no way to pass a
passphrase. It is written to a private temporary directory only for the
duration of the CCP call and removed straight after.

As with the CSM fetcher this replaces:

* the CA bundle for the CCP server is resolved from candidate paths;
* failures are classified onto the catalogue, so a rejected certificate
  reports as AUTHENTICATION_FAILURE rather than as a bare ``RuntimeError``;
* the credential is cached for the process lifetime, because a service-mode
  task must not re-hit CCP on every poll. ``get_credentials(refresh=True)``
  re-reads both the certificate and the account, which picks up a rotated
  password or a renewed certificate.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError

from utility import failure_catalog as catalog
from utility.error_classifier import PreflightError
from utility.connector_config import CyberArkSettings

logger = logging.getLogger(__name__)

#: Path of the client-certificate-authenticated CCP REST endpoint, relative to
#: ``base_url``. The non-certificate variant (``/AIMWebService/...``) relies on
#: an OS-user restriction, which has no meaning inside a container.
ACCOUNTS_PATH = "/AIMWebService_certs/api/Accounts"

#: Searched in order. The first that exists wins.
CA_BUNDLE_CANDIDATES: List[str] = [
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/ssl/certs/ca-certificates.crt",
    "/opt/certs/tls-ca-bundle.pem",
]

_CERT_FIELD = "client_cert_pem"
_KEY_FIELD = "client_key_pem"


@dataclass(frozen=True)
class Credentials:
    username: str
    password: str

    def principal(self, realm: str) -> str:
        """Fully qualified BSP principal, e.g. ``svcaccount@INTRANET.BARCAPINT.COM``."""
        return self.username if "@" in self.username else f"{self.username}{realm}"

    def __repr__(self) -> str:  # keep the password out of tracebacks and logs
        return f"Credentials(username={self.username!r}, password=***)"


@dataclass(frozen=True)
class _ClientCertificate:
    cert_pem: str
    key_pem: str

    def __repr__(self) -> str:  # keep the key out of tracebacks and logs
        return "_ClientCertificate(cert_pem=..., key_pem=***)"


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


@contextmanager
def _materialised(certificate: _ClientCertificate) -> Iterator[Tuple[str, str]]:
    """Write the certificate and key to owner-only files; remove them on exit."""
    directory = tempfile.mkdtemp(prefix="cyberark-ccp-")  # created 0700
    try:
        paths = []
        for name, content in (("client_cert.pem", certificate.cert_pem), ("client_key.pem", certificate.key_pem)):
            path = os.path.join(directory, name)
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(content)
            paths.append(path)
        yield paths[0], paths[1]
    finally:
        shutil.rmtree(directory, ignore_errors=True)


class CyberArkAuthenticator:
    def __init__(self, settings: CyberArkSettings, *, session: Optional[Any] = None):
        self._settings = settings
        self._session = session or boto3.Session()
        self._lock = threading.Lock()
        self._cached: Optional[Credentials] = None

    # -- request shape -----------------------------------------------------

    @property
    def accounts_url(self) -> str:
        return f"{self._settings.base_url.rstrip('/')}{ACCOUNTS_PATH}"

    @property
    def _query(self) -> Dict[str, str]:
        # Passed as params so requests URL-encodes object names with spaces.
        return {
            "AppID": self._settings.app_id,
            "Safe": self._settings.safe,
            "Folder": self._settings.folder,
            "Object": self._settings.object,
        }

    @property
    def _context(self) -> Dict[str, Any]:
        """Identifies the request in errors and logs. Carries no secret."""
        return {"accounts_url": self.accounts_url, **self._query}

    @property
    def _verify(self) -> Any:
        return resolve_ca_bundle(self._settings.ca_bundle_path, ssl_verify=self._settings.ssl_verify)

    # -- flow --------------------------------------------------------------

    def _load_client_certificate(self) -> _ClientCertificate:
        secret_id = self._settings.client_cert_secret_id
        context = {"secret_id": secret_id, "region": self._settings.secret_region}
        logger.info("Reading CyberArk client certificate from Secrets Manager", extra=context)

        try:
            client = self._session.client("secretsmanager", region_name=self._settings.secret_region)
            response = client.get_secret_value(SecretId=secret_id)
        except ClientError as exc:
            # AccessDenied / ResourceNotFound / KMS decrypt failures: the task
            # role or the secret is misconfigured, which retrying will not fix.
            raise PreflightError(
                f"Could not read the CyberArk client certificate secret: "
                f"{exc.response.get('Error', {}).get('Code', 'ClientError')}",
                catalog.AUTHENTICATION_FAILURE,
                context=context,
                cause=exc,
            ) from exc
        except BotoCoreError as exc:
            raise PreflightError(
                f"Secrets Manager request failed: {exc}",
                catalog.NETWORK_FAILURE,
                context=context,
                cause=exc,
            ) from exc

        try:
            payload = json.loads(response.get("SecretString") or "")
        except ValueError as exc:
            raise PreflightError(
                "CyberArk client certificate secret is not a JSON SecretString",
                catalog.AUTHENTICATION_FAILURE,
                context=context,
                cause=exc,
            ) from exc

        if not isinstance(payload, dict) or not payload.get(_CERT_FIELD) or not payload.get(_KEY_FIELD):
            raise PreflightError(
                f"CyberArk client certificate secret must hold {_CERT_FIELD} and {_KEY_FIELD}",
                catalog.AUTHENTICATION_FAILURE,
                context={**context, "keys_present": sorted(payload) if isinstance(payload, dict) else []},
            )

        if "ENCRYPTED" in payload[_KEY_FIELD]:
            raise PreflightError(
                "CyberArk client key is passphrase-protected; store it as an unencrypted PEM",
                catalog.AUTHENTICATION_FAILURE,
                context=context,
            )

        return _ClientCertificate(cert_pem=payload[_CERT_FIELD], key_pem=payload[_KEY_FIELD])

    def _fetch_account(self, certificate: _ClientCertificate) -> Credentials:
        logger.info("Retrieving system-account credential from CyberArk CCP", extra=self._context)

        try:
            with _materialised(certificate) as client_cert:
                response = requests.get(
                    self.accounts_url,
                    params=self._query,
                    cert=client_cert,
                    verify=self._verify,
                    timeout=self._settings.request_timeout,
                )
        except requests.exceptions.SSLError as exc:
            # A handshake failure here is almost always the certificate: an
            # untrusted CCP server chain, or a client certificate CCP rejects.
            raise PreflightError(
                f"TLS handshake with CyberArk CCP failed: {exc}",
                catalog.AUTHENTICATION_FAILURE,
                context=self._context,
                cause=exc,
            ) from exc
        except requests.RequestException as exc:
            raise PreflightError(
                f"CyberArk CCP request failed: {exc}",
                catalog.NETWORK_FAILURE,
                context=self._context,
                cause=exc,
            ) from exc

        if response.status_code != 200:
            error: Dict[str, Any] = {}
            try:
                body = response.json()
                if isinstance(body, dict):
                    # CCP errors name the failing AppID/Safe/restriction and carry
                    # no secret material, so they are safe and useful to keep.
                    error = {"error_code": body.get("ErrorCode"), "error_msg": body.get("ErrorMsg")}
            except ValueError:
                error = {"response": response.text[:500]}
            raise PreflightError(
                f"CyberArk CCP returned HTTP {response.status_code}"
                + (f" ({error['error_code']})" if error.get("error_code") else ""),
                catalog.AUTHENTICATION_FAILURE,
                context={**self._context, "status": response.status_code, **error},
            )

        try:
            body = response.json()
        except ValueError as exc:
            raise PreflightError(
                "CyberArk CCP response is not JSON",
                catalog.AUTHENTICATION_FAILURE,
                context=self._context,
                cause=exc,
            ) from exc

        if not isinstance(body, dict) or not body.get("UserName") or not body.get("Content"):
            raise PreflightError(
                "CyberArk CCP response is missing UserName/Content",
                catalog.AUTHENTICATION_FAILURE,
                context={**self._context, "keys_present": sorted(body) if isinstance(body, dict) else []},
            )

        return Credentials(username=body["UserName"], password=body["Content"])

    def get_credentials(self, *, refresh: bool = False) -> Credentials:
        with self._lock:
            if self._cached is not None and not refresh:
                return self._cached
            self._cached = self._fetch_account(self._load_client_certificate())
            logger.info(
                "CyberArk credentials retrieved",
                extra={"cyberark_username": self._cached.username},
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
