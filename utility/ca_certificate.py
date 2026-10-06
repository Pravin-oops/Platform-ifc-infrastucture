"""Barclays root CA, fetched from Secrets Manager at container start.

The CA (``CARoot.pem``) is neither committed nor baked into the image. It is
one Secrets Manager secret, holding the plain PEM text as its SecretString,
read with the ECS task role like the CyberArk client certificate. It is written
to ``ca_certificate.path`` before the BSP client is built, because two readers
expect a real file there:

* ``ssl.ca.location`` in the BSP client YAML (``utility/bsp_sit_config.yaml``),
  which librdkafka reads for the broker TLS;
* ``schema_registry.ca_location``, which verifies the Schema Registry.

A CA certificate is public, so unlike the CyberArk key it is written readable
and left in place for the life of the container.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from utility import failure_catalog as catalog
from utility.connector_config import CaCertificateSettings
from utility.error_classifier import PreflightError

logger = logging.getLogger(__name__)


def install(settings: CaCertificateSettings, *, session: Optional[Any] = None) -> Optional[str]:
    """Write the CA from its secret to ``settings.path`` and return that path.

    Returns None, writing nothing, when no secret is configured: a local run
    that points the CA paths at a file of its own.
    """
    if not settings.secret_id:
        logger.warning(
            "ca_certificate.secret_id is not set; expecting the CA files at the configured "
            "paths already"
        )
        return None

    pem = _read_secret(settings, session or boto3.Session())
    _write(settings.path, pem)

    logger.info(
        "Barclays root CA written from Secrets Manager",
        extra={"secret_id": settings.secret_id, "path": settings.path},
    )
    return settings.path


def _read_secret(settings: CaCertificateSettings, session: Any) -> str:
    context = {"secret_id": settings.secret_id, "region": settings.secret_region}
    logger.info("Reading the Barclays root CA from Secrets Manager", extra=context)

    try:
        client = session.client("secretsmanager", region_name=settings.secret_region)
        response = client.get_secret_value(SecretId=settings.secret_id)
    except ClientError as exc:
        # AccessDenied / ResourceNotFound / KMS decrypt: the task role or the
        # secret is misconfigured, which retrying will not fix. Filed as an
        # authentication (truststore) failure, as the CyberArk secrets are.
        raise PreflightError(
            "Could not read the CA certificate secret: "
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

    secret = response.get("SecretString")
    if secret is None and response.get("SecretBinary") is not None:
        secret = response["SecretBinary"].decode("utf-8", errors="replace")
    pem = (secret or "").strip()

    if "-----BEGIN CERTIFICATE-----" not in pem:
        raise PreflightError(
            "CA certificate secret is empty or not a PEM (no 'BEGIN CERTIFICATE' block)",
            catalog.AUTHENTICATION_FAILURE,
            context=context,
        )
    if "PRIVATE KEY-----" in pem:
        # Never write a key to a world-readable trust file; never log its value.
        raise PreflightError(
            "CA certificate secret contains a private key; store only the CA certificate",
            catalog.AUTHENTICATION_FAILURE,
            context=context,
        )
    return pem + "\n"


def _write(path: str, pem: str) -> None:
    directory = os.path.dirname(path) or "."
    try:
        os.makedirs(directory, exist_ok=True)
        # Written beside the target and renamed, so a reader never sees half a file.
        fd, staging = tempfile.mkstemp(dir=directory, prefix=".ca-", suffix=".pem")
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(pem)
        os.chmod(staging, 0o644)
        os.replace(staging, path)
    except OSError as exc:
        raise PreflightError(
            f"Could not write the CA certificate to {path}: {exc}",
            catalog.CONTAINER_FAILURE,
            context={"path": path},
            cause=exc,
        ) from exc
