from __future__ import annotations

import base64
import binascii
import hashlib
import logging
import os
import re
from typing import Any, Callable, Dict, List, Optional

from botocore.exceptions import BotoCoreError, ClientError

from utility import ca_certificate
from utility import failure_catalog as catalog
from utility.auth_helper import BSPClient
from utility.connector_config import ConnectorSettings
from utility.connector_utility import SourceAccessError, materialise_local, package_resource
from utility.cyberark_ccp_fetch import CyberArkAuthenticator
from utility.error_classifier import ConnectorError, classify
from utility.kafka_factory import KafkaStackFactory
from utility.observability_utility import Metrics
from utility.resilience_utility import ShutdownSignal
from utility.trigger_source import make_source

logger = logging.getLogger(__name__)

PASSED = "PASSED"
FAILED = "FAILED"
SKIPPED = "SKIPPED"

OUTCOME_PASSED = "CONNECTIVITY_CHECK_PASSED"
OUTCOME_FAILED = "CONNECTIVITY_CHECK_FAILED"

_PEM_CERTIFICATE = re.compile(r"-----BEGIN CERTIFICATE-----(.+?)-----END CERTIFICATE-----", re.S)
_PEM_KEY_TYPE = re.compile(r"-----BEGIN ([A-Z ]*PRIVATE KEY)-----")
_SENSITIVE_KEY = re.compile(r"password|secret|token|credential", re.IGNORECASE)
_PLAIN = (str, int, float, bool, type(None))


def mask(value: Optional[str]) -> str:
    if not value:
        return "<not set>"
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
    return f"<{len(value)} chars, sha256 {digest}>"


def certificate_fingerprints(pem: str) -> List[str]:
    fingerprints = []
    for body in _PEM_CERTIFICATE.findall(pem):
        try:
            der = base64.b64decode("".join(body.split()), validate=True)
        except (binascii.Error, ValueError):
            fingerprints.append("<unreadable>")
            continue
        fingerprints.append(":".join(f"{b:02X}" for b in hashlib.sha256(der).digest()))
    return fingerprints


def redacted(config: Dict[str, Any]) -> Dict[str, Any]:
    shown: Dict[str, Any] = {}
    for key, value in sorted(config.items()):
        if callable(value):
            shown[key] = f"<callable {getattr(value, '__name__', type(value).__name__)}>"
        elif _SENSITIVE_KEY.search(key):
            shown[key] = mask(str(value))
        elif isinstance(value, _PLAIN):
            shown[key] = value
        else:
            shown[key] = f"<{type(value).__name__}>"
    return shown


class _Skipped(Exception):
    pass


class _Failed(Exception):
    def __init__(self, detail: str, scenario: catalog.Scenario, details: Dict[str, Any]):
        super().__init__(detail)
        self.scenario = scenario
        self.details = details


class ConnectivityCheck:
    def __init__(self, settings: ConnectorSettings):
        self._settings = settings
        self._results: List[Dict[str, Any]] = []
        cyberark = settings.cyberark
        self._authenticator = (
            CyberArkAuthenticator(cyberark) if cyberark is not None and cyberark.enabled else None
        )

    def _step(self, name: str, fn: Callable[[], Dict[str, Any]]) -> bool:
        try:
            details = fn()
        except _Skipped as reason:
            self._record(name, SKIPPED, str(reason), {})
            return False
        except _Failed as failure:
            self._record(
                name,
                FAILED,
                str(failure),
                {**failure.details, "scenario": failure.scenario.key,
                 "exit_code": failure.scenario.exit_code},
            )
            return False
        except Exception as exc:
            classification = classify(exc, operation=f"connectivity_check:{name}")
            self._record(
                name,
                FAILED,
                str(exc),
                {"scenario": classification.scenario.key, "exit_code": classification.exit_code},
            )
            return False

        self._record(name, PASSED, "ok", details)
        return True

    def _record(self, name: str, status: str, detail: str, details: Dict[str, Any]) -> None:
        result = {"check": name, "status": status, "detail": detail, **details}
        self._results.append(result)
        logger.log(
            logging.ERROR if status == FAILED else logging.INFO,
            "Connectivity check %s: %s - %s%s",
            name,
            status,
            detail,
            "".join(f"\n  {key}={value}" for key, value in details.items()),
            extra={"connectivity_check": result},
        )

    def _ca_certificate(self) -> Dict[str, Any]:
        ca = self._settings.ca_certificate
        if not ca.secret_id:
            raise _Skipped("ca_certificate.secret_id is not set")

        path = ca_certificate.install(ca)
        with open(path, "r", encoding="utf-8") as handle:
            pem = handle.read()
        return {
            "secret_id": ca.secret_id,
            "region": ca.secret_region,
            "written_to": path,
            "bytes": len(pem),
            "certificates": len(certificate_fingerprints(pem)),
            "sha256_fingerprints": certificate_fingerprints(pem),
        }

    def _cyberark_client_certificate(self) -> Dict[str, Any]:
        if self._authenticator is None:
            raise _Skipped("CyberArk is not enabled")

        cyberark = self._settings.cyberark
        certificate = self._authenticator._load_client_certificate()
        key_type = _PEM_KEY_TYPE.search(certificate.key_pem)
        return {
            "region": cyberark.secret_region,
            "client_cert_secret_id": cyberark.client_cert_secret_id,
            "client_cert_sha256_fingerprints": certificate_fingerprints(certificate.cert_pem),
            "client_key_secret_id": cyberark.client_key_secret_id,
            "client_key_type": key_type.group(1) if key_type else "<unknown>",
            "client_key": mask(certificate.key_pem),
        }

    def _cyberark_credentials(self) -> Dict[str, Any]:
        authenticator = self._authenticator
        if authenticator is None:
            if not self._settings.kafka.bsp_config_path:
                raise _Skipped("CyberArk is not enabled and there is no BSP config to log in to")
            username, password = os.environ.get("BSP_USERNAME"), os.environ.get("BSP_PASSWORD")
            if not username or not password:
                raise ConnectorError(
                    "CyberArk is not enabled and BSP_USERNAME/BSP_PASSWORD are not both set",
                    catalog.AUTHENTICATION_FAILURE,
                )
            return {"source": "environment", "bsp_username": username, "bsp_password": mask(password)}

        cyberark = self._settings.cyberark
        credentials = authenticator.export_to_environment(cyberark.principal_realm)
        return {
            "source": "CyberArk CCP",
            "accounts_url": authenticator.accounts_url,
            "app_id": cyberark.app_id,
            "safe": cyberark.safe,
            "folder": cyberark.folder,
            "object": cyberark.object,
            "username": credentials.username,
            "bsp_username": os.environ.get("BSP_USERNAME"),
            "bsp_password": mask(os.environ.get("BSP_PASSWORD")),
        }

    def _source_table(self) -> Dict[str, Any]:
        settings = self._settings
        athena = settings.source.athena
        source = make_source(settings.source, trigger=settings.trigger)
        details: Dict[str, Any] = {
            "trigger": settings.trigger,
            "table": source.table,
            "catalog": athena.catalog,
            "workgroup": athena.workgroup,
            "output_location": athena.output_location,
            "business_date": source.business_date.isoformat(),
        }
        try:
            source.check_access()
        except (ClientError, BotoCoreError) as exc:
            raise SourceAccessError(
                f"Athena table metadata for {source.table} could not be read: {exc}",
                path=source.table,
                operation="read",
                cause=exc,
            ) from exc

        details["row_count"] = source.count()
        details["query_execution_id"] = source.last_query_execution_id
        if not details["row_count"]:
            details["note"] = "the table is readable but has no rows for this business_date"
        return details

    def _bsp(self) -> Dict[str, Any]:
        settings = self._settings
        factory = KafkaStackFactory(
            settings,
            metrics=Metrics(dimensions={"Environment": settings.app.environment}),
            shutdown=ShutdownSignal(),
        )

        bsp = None
        if settings.kafka.bsp_config_path:
            bsp = BSPClient(materialise_local(package_resource(settings.kafka.bsp_config_path)))

        try:
            config = factory.probe(bsp)
        finally:
            factory.report.log_summary()

        report = factory.report
        details: Dict[str, Any] = {
            "connection": "BSP" if bsp is not None else "direct (no BSP)",
            "bsp_config_path": settings.kafka.bsp_config_path,
            "topic": settings.kafka.topic,
            "producer_config": redacted(config),
            "checks": [
                f"{r.name}: {'ok' if r.passed else 'FAILED'} ({r.detail})" for r in report.results
            ],
        }
        auth = next((r for r in report.results if r.name == "auth:bam_token" and r.passed), None)
        if auth is not None:
            details["bam_token_seconds_remaining"] = auth.context.get("token_seconds_remaining")
            details["bam_token_claims"] = auth.context.get("token_claims")

        if report.failures:
            first = report.failures[0]
            raise _Failed(f"{first.name}: {first.detail}", first.scenario or catalog.UNKNOWN, details)
        return details

    def run(self) -> Dict[str, Any]:
        settings = self._settings
        logger.warning(
            "Connectivity check mode (run.connectivity_check=true): checking Secrets Manager, "
            "CyberArk, BSP and read access to the source table (a COUNT only). Nothing is "
            "published to %s",
            settings.kafka.topic,
        )

        self._step("aws_secret:ca_certificate", self._ca_certificate)
        certificate_read = self._step(
            "aws_secret:cyberark_client_certificate", self._cyberark_client_certificate
        )
        if self._authenticator is not None and not certificate_read:
            self._record(
                "cyberark:credentials", SKIPPED, "the CyberArk client certificate could not be read", {}
            )
        else:
            self._step("cyberark:credentials", self._cyberark_credentials)
        self._step("source:table", self._source_table)
        self._step("bsp:kafka", self._bsp)

        failed = [r for r in self._results if r["status"] == FAILED]
        exit_code = failed[0].get("exit_code", catalog.UNKNOWN.exit_code) if failed else catalog.EXIT_OK
        outcome = OUTCOME_FAILED if failed else OUTCOME_PASSED

        logger.log(
            logging.ERROR if failed else logging.INFO,
            "Connectivity check finished: %s exit_code=%s | %s",
            outcome,
            exit_code,
            ", ".join(f"{r['check']} {r['status']}" for r in self._results),
        )
        return {
            "outcome": outcome,
            "exit_code": exit_code,
            "published": 0,
            "topic": settings.kafka.topic,
            "environment": settings.app.environment,
            "connectivity_checks": self._results,
        }


def run(settings: ConnectorSettings) -> Dict[str, Any]:
    return ConnectivityCheck(settings).run()
