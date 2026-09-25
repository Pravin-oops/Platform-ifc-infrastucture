"""BSP Schema Registry REST client.

The secure registry (port 8095) accepts a BAM bearer token and rejects basic
auth, so there is deliberately no username/password path here.

Beyond fetching the schema id needed for the Confluent wire format, this client
compares the locally bundled ``.avsc`` with the registered subject at startup.
That check is what turns "Schema Validation Failure" from a per-message
production incident into a deployment that refuses to start.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from utility import failure_catalog as catalog
from utility.error_classifier import ConnectorError, PreflightError
from utility.resilience_utility import BackoffPolicy, ShutdownSignal, retry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RegisteredSchema:
    subject: str
    schema_id: int
    version: int
    schema: Dict[str, Any]


class SchemaRegistryClient:
    def __init__(
        self,
        base_url: str,
        *,
        token_provider: Any,
        ca_location: Optional[str] = None,
        timeout: int = 30,
        backoff: Optional[BackoffPolicy] = None,
        attempts: int = 4,
        shutdown: Optional[ShutdownSignal] = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._tokens = token_provider
        self._verify: Any = ca_location if ca_location else True
        self._timeout = timeout
        self._backoff = backoff or BackoffPolicy()
        self._attempts = attempts
        self._shutdown = shutdown

    # -- transport ---------------------------------------------------------

    def _request(self, path: str, *, force_refresh: bool = False) -> Any:
        url = f"{self._base_url}{path}"
        token = self._tokens.get(force_refresh=force_refresh)

        try:
            response = requests.get(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/vnd.schemaregistry.v1+json",
                },
                verify=self._verify,
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            raise ConnectorError(
                f"Schema Registry request to {path} failed: {exc}",
                catalog.SCHEMA_REGISTRY_UNAVAILABLE,
                context={"url": url},
                cause=exc,
            ) from exc

        if response.status_code == 200:
            return response.json()

        if response.status_code in (401, 403) and not force_refresh:
            # The cached token may have been revoked or rotated early; one
            # forced refresh distinguishes an expiry from a real ACL problem.
            logger.warning(
                "Schema Registry returned %s; refreshing the BAM token and retrying once",
                response.status_code,
            )
            return self._request(path, force_refresh=True)

        scenario = {
            401: catalog.AUTHENTICATION_FAILURE,
            403: catalog.AUTHORISATION_FAILURE,
            404: catalog.SCHEMA_VALIDATION_FAILURE,
        }.get(response.status_code, catalog.SCHEMA_REGISTRY_UNAVAILABLE)

        raise ConnectorError(
            f"Schema Registry returned HTTP {response.status_code} for {path}",
            scenario,
            context={"url": url, "status": response.status_code, "body": response.text[:500]},
        )

    def _get_with_retry(self, path: str, description: str) -> Any:
        def retry_on(exc: BaseException) -> bool:
            return isinstance(exc, ConnectorError) and exc.scenario.retryable

        return retry(
            lambda: self._request(path),
            attempts=self._attempts,
            policy=self._backoff,
            retry_on=retry_on,
            shutdown=self._shutdown,
            description=description,
        )

    # -- API ---------------------------------------------------------------

    def latest_schema(self, subject: str) -> RegisteredSchema:
        document = self._get_with_retry(
            f"/subjects/{subject}/versions/latest", f"schema registry lookup for {subject}"
        )

        try:
            schema = json.loads(document["schema"])
        except (KeyError, ValueError) as exc:
            raise PreflightError(
                f"Schema Registry response for {subject} did not contain a parsable schema",
                catalog.SCHEMA_VALIDATION_FAILURE,
                context={"subject": subject},
                cause=exc,
            ) from exc

        registered = RegisteredSchema(
            subject=subject,
            schema_id=int(document["id"]),
            version=int(document.get("version", 0)),
            schema=schema,
        )
        logger.info(
            "Resolved registry subject",
            extra={
                "subject": subject,
                "schema_id": registered.schema_id,
                "schema_version": registered.version,
            },
        )
        return registered

    def subjects(self) -> List[str]:
        return list(self._get_with_retry("/subjects", "schema registry subject list"))


def value_subject(topic: str) -> str:
    """TopicNameStrategy, which is what BSP registers subjects under."""
    return f"{topic}-value"


# ---------------------------------------------------------------------------
# Local schema vs registered schema
# ---------------------------------------------------------------------------


def _field_types(schema: Dict[str, Any]) -> Dict[str, Any]:
    return {field["name"]: field.get("type") for field in schema.get("fields", [])}


def _optional_fields(schema: Dict[str, Any]) -> Set[str]:
    """Fields that a consumer can do without: nullable, or carrying a default."""
    optional: Set[str] = set()
    for field in schema.get("fields", []):
        type_ = field.get("type")
        nullable = isinstance(type_, list) and "null" in type_
        if nullable or "default" in field:
            optional.add(field["name"])
    return optional


def _enum_of(type_: Any) -> Optional[Dict[str, Any]]:
    """The enum definition inside a field type, unwrapping a nullable union."""
    candidates = type_ if isinstance(type_, list) else [type_]
    for candidate in candidates:
        if isinstance(candidate, dict) and candidate.get("type") == "enum":
            return candidate
    return None


def _compare_enum(name: str, local: Dict[str, Any], registered: Dict[str, Any]) -> List[str]:
    """Compare two enum field types symbol by symbol.

    Whole-dict equality is the wrong test: ``doc`` strings, key order and
    symbol order all differ harmlessly between a hand-maintained .avsc and what
    the registry returns, while the one difference that matters - a symbol the
    producer writes that the registry does not know - is invisible in a diff of
    the JSON. A record carrying an unregistered symbol is unencodable by the
    consumer, so that direction is blocking; extra registry symbols are not.
    """
    findings: List[str] = []
    local_symbols = list(local.get("symbols", []))
    registered_symbols = set(registered.get("symbols", []))

    producer_only = [s for s in local_symbols if s not in registered_symbols]
    if producer_only:
        findings.append(
            f"BLOCKING: enum '{name}' has symbols locally that the registered subject does "
            f"not carry: {sorted(producer_only)}"
        )

    registry_only = sorted(registered_symbols - set(local_symbols))
    if registry_only:
        findings.append(
            f"INFO: registered enum '{name}' carries symbols this connector does not "
            f"publish: {registry_only}"
        )

    if local.get("name") != registered.get("name"):
        findings.append(
            f"INFO: enum '{name}' type name differs - local {local.get('name')!r} vs "
            f"registered {registered.get('name')!r}"
        )
    return findings


def compare_schemas(local: Dict[str, Any], registered: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Compare the bundled schema with the registered one.

    Not a full Avro resolution check - that is the registry's job. This catches
    the drift that actually happens in practice and that the registry will not
    catch for us, because the producer writes with the *local* schema under the
    *registered* schema's id:

    * a field the producer will write that the registry does not know about
      (the consumer decodes with the registered schema and mis-reads the record);
    * a mandatory registered field the producer does not populate;
    * a type change on a shared field.

    Returns ``(compatible, findings)``. Findings are returned even when
    compatible, so additive registry changes are logged rather than hidden.
    """
    findings: List[str] = []

    local_types = _field_types(local)
    registered_types = _field_types(registered)

    producer_only = set(local_types) - set(registered_types)
    registry_only = set(registered_types) - set(local_types)
    optional_in_registry = _optional_fields(registered)

    for name in sorted(producer_only):
        findings.append(
            f"BLOCKING: field '{name}' exists locally but not in the registered subject; "
            "the record would be written under a schema id that cannot decode it"
        )

    for name in sorted(registry_only):
        if name in optional_in_registry:
            findings.append(f"INFO: registered field '{name}' is not produced, but is optional")
        else:
            findings.append(
                f"BLOCKING: registered field '{name}' is mandatory but is not produced"
            )

    for name in sorted(set(local_types) & set(registered_types)):
        local_enum = _enum_of(local_types[name])
        registered_enum = _enum_of(registered_types[name])
        if local_enum and registered_enum:
            findings.extend(_compare_enum(name, local_enum, registered_enum))
            continue
        if bool(local_enum) != bool(registered_enum):
            findings.append(
                f"BLOCKING: field '{name}' is an enum on one side only - local "
                f"{local_types[name]!r} vs registered {registered_types[name]!r}"
            )
            continue
        if local_types[name] != registered_types[name]:
            findings.append(
                f"BLOCKING: field '{name}' type differs - local {local_types[name]!r} "
                f"vs registered {registered_types[name]!r}"
            )

    local_name = f"{local.get('namespace', '')}.{local.get('name', '')}"
    registered_name = f"{registered.get('namespace', '')}.{registered.get('name', '')}"
    if local_name != registered_name:
        # Not blocking: Avro resolves records by field, not by full name, and
        # the topic document itself uses different namespaces across schemas.
        findings.append(
            f"INFO: record name differs - local {local_name!r} vs registered {registered_name!r}"
        )

    compatible = not any(f.startswith("BLOCKING") for f in findings)
    return compatible, findings