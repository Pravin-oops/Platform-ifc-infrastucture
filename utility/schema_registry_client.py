"""BSP Schema Registry REST client."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

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
    #: The registry node that answered.
    url: str = ""


class SchemaRegistryClient:
    """Reads from one registry node, or fails over across several."""

    def __init__(
        self,
        base_url: Union[str, Sequence[str]],
        *,
        token_provider: Any = None,
        ca_location: Optional[str] = None,
        timeout: int = 30,
        backoff: Optional[BackoffPolicy] = None,
        attempts: int = 4,
        shutdown: Optional[ShutdownSignal] = None,
    ):
        urls = [base_url] if isinstance(base_url, str) else list(base_url)
        self._urls = [url.strip().rstrip("/") for url in urls if url and url.strip()]
        if not self._urls:
            raise ValueError("SchemaRegistryClient needs at least one registry URL")
        self._preferred = self._urls[0]
        #: None for the DEV registry (8082), which takes no bearer token.
        self._tokens = token_provider
        self._verify: Any = ca_location if ca_location else True
        self._timeout = timeout
        self._backoff = backoff or BackoffPolicy()
        self._attempts = attempts
        self._shutdown = shutdown

    @property
    def urls(self) -> List[str]:
        return list(self._urls)

    def _ordered_urls(self) -> List[str]:
        return [self._preferred] + [url for url in self._urls if url != self._preferred]

    def _request(self, path: str) -> Tuple[Any, str]:
        """GET ``path`` from the first node that answers; returns (body, node)."""
        last_error: Optional[ConnectorError] = None

        for base in self._ordered_urls():
            try:
                body = self._request_node(base, path)
            except ConnectorError as exc:
                if exc.scenario is not catalog.SCHEMA_REGISTRY_UNAVAILABLE:
                    raise
                last_error = exc
                if len(self._urls) > 1:
                    logger.warning(
                        "Schema Registry node %s is unavailable (%s); trying the next node",
                        base,
                        exc,
                    )
                continue

            if base != self._preferred:
                logger.warning("Schema Registry failed over to %s", base)
                self._preferred = base
            return body, base

        assert last_error is not None
        if len(self._urls) > 1:
            raise ConnectorError(
                f"No Schema Registry node answered ({', '.join(self._urls)}): {last_error}",
                catalog.SCHEMA_REGISTRY_UNAVAILABLE,
                context={"urls": self._urls},
                cause=last_error,
            ) from last_error
        raise last_error

    def _request_node(self, base: str, path: str, *, force_refresh: bool = False) -> Any:
        url = f"{base}{path}"
        headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        if self._tokens is not None:
            headers["Authorization"] = f"Bearer {self._tokens.get(force_refresh=force_refresh)}"

        try:
            response = requests.get(
                url,
                headers=headers,
                verify=self._verify,
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            raise ConnectorError(
                f"Schema Registry request to {url} failed: {exc}",
                catalog.SCHEMA_REGISTRY_UNAVAILABLE,
                context={"url": url},
                cause=exc,
            ) from exc

        if response.status_code == 200:
            return response.json()

        if response.status_code in (401, 403) and not force_refresh and self._tokens is not None:
            # The cached token may have been revoked or rotated early; one
            # forced refresh distinguishes an expiry from a real ACL problem.
            logger.warning(
                "Schema Registry returned %s; refreshing the BAM token and retrying once",
                response.status_code,
            )
            return self._request_node(base, path, force_refresh=True)

        scenario = {
            401: catalog.AUTHENTICATION_FAILURE,
            403: catalog.AUTHORISATION_FAILURE,
            404: catalog.SCHEMA_VALIDATION_FAILURE,
        }.get(response.status_code, catalog.SCHEMA_REGISTRY_UNAVAILABLE)

        raise ConnectorError(
            f"Schema Registry returned HTTP {response.status_code} for {url}",
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

    def latest_schema(self, subject: str) -> RegisteredSchema:
        document, node = self._get_with_retry(
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
            url=node,
        )
        logger.debug(
            "Resolved registry subject",
            extra={
                "subject": subject,
                "schema_id": registered.schema_id,
                "schema_version": registered.version,
                "registry_url": node,
            },
        )
        return registered

    def subjects(self) -> List[str]:
        subjects, _node = self._get_with_retry("/subjects", "schema registry subject list")
        return list(subjects)


def value_subject(topic: str) -> str:
    """TopicNameStrategy, which is what BSP registers subjects under."""
    return f"{topic}-value"



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
    """Compare two enum field types symbol by symbol."""
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
    """Compare the bundled schema with the registered one."""
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