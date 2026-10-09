from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from utility import failure_catalog as catalog
from utility.failure_catalog import Scenario

logger = logging.getLogger(__name__)


class ConnectorError(RuntimeError):
    def __init__(
        self,
        message: str,
        scenario: Scenario,
        *,
        context: Optional[Dict[str, Any]] = None,
        cause: Optional[BaseException] = None,
    ):
        super().__init__(message)
        self.scenario = scenario
        self.context = context or {}
        self.cause = cause

    @property
    def exit_code(self) -> int:
        return self.scenario.exit_code


class PreflightError(ConnectorError):
    pass


class PublishError(ConnectorError):
    pass


class RecordRejected(Exception):
    def __init__(self, message: str, scenario: Scenario, *, detail: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.scenario = scenario
        self.detail = detail or {}


class ZeroRecordsError(ConnectorError):
    pass


_PATTERNS: Sequence[Tuple[Scenario, List[str]]] = (
    (
        catalog.MESSAGE_TOO_LARGE,
        ["MSG_SIZE_TOO_LARGE", "RECORDTOOLARGE", "MESSAGE SIZE EXCEEDS", "MESSAGE_TOO_LARGE"],
    ),
    (
        catalog.AUTHORISATION_FAILURE,
        [
            "TOPIC_AUTHORIZATION_FAILED", "TOPICAUTHORIZATIONEXCEPTION",
            "CLUSTER_AUTHORIZATION_FAILED", "CLUSTERAUTHORIZATIONEXCEPTION",
            "GROUP_AUTHORIZATION_FAILED", "NOT AUTHORIZED", "NOT AUTHORISED",
            "ACCESS FORBIDDEN", "HTTP 403", "STATUS=403",
        ],
    ),
    (
        catalog.AUTHENTICATION_FAILURE,
        [
            "SASLAUTHENTICATIONEXCEPTION", "_AUTHENTICATION", "SASL AUTHENTICATION",
            "SSLHANDSHAKEEXCEPTION", "SSL HANDSHAKE", "CERTIFICATE VERIFY FAILED",
            "AUTHENTICATION FAILED", "INVALID_GRANT", "UNAUTHORIZED", "HTTP 401",
            "STATUS=401", "MALFORMED JWT", "TOKEN EXPIRED", "BAM TOKEN",
            "SSL.CA.LOCATION FAILED",
        ],
    ),
    (
        catalog.TOPIC_UNAVAILABLE,
        ["UNKNOWN_TOPIC", "UNKNOWNTOPICORPARTITION", "UNKNOWN TOPIC OR PARTITION"],
    ),
    (
        catalog.SCHEMA_VALIDATION_FAILURE,
        [
            "SERIALIZATIONEXCEPTION", "SCHEMA COMPATIBILITY", "INCOMPATIBLE SCHEMA",
            "IS NOT AN EXAMPLE OF THE SCHEMA", "VALIDATIONERROR", "INVALID PAYLOAD",
            "SCHEMA MISMATCH", "SCHEMA DRIFT", "SCHEMAPARSEEXCEPTION", "_INVALID_ARG",
        ],
    ),
    (
        catalog.SCHEMA_REGISTRY_UNAVAILABLE,
        [
            "SCHEMA REGISTRY", "SCHEMAREGISTRY", "HTTP 502", "HTTP 503", "HTTP 504",
            "STATUS=502", "STATUS=503", "STATUS=504", "BAD GATEWAY", "SERVICE UNAVAILABLE",
        ],
    ),
    (
        catalog.PARTITION_LEADER_FAILURE,
        [
            "NOTLEADERFORPARTITION", "NOT_LEADER_FOR_PARTITION", "LEADER_NOT_AVAILABLE",
            "LEADER NOT AVAILABLE", "NOT_ENOUGH_REPLICAS", "REQUEST_TIMED_OUT_PER_PARTITION",
        ],
    ),
    (
        catalog.BROKER_UNAVAILABLE,
        [
            "ALL_BROKERS_DOWN", "ALL BROKERS DOWN", "_TRANSPORT", "BROKER TRANSPORT FAILURE",
            "NETWORKEXCEPTION", "DISCONNECTEXCEPTION", "DISCONNECTED", "_ALL_BROKERS_DOWN",
        ],
    ),
    (
        catalog.NETWORK_FAILURE,
        [
            "NAME OR SERVICE NOT KNOWN", "GETADDRINFO", "TEMPORARY FAILURE IN NAME RESOLUTION",
            "NO ROUTE TO HOST", "NETWORK IS UNREACHABLE", "CONNECTION REFUSED",
            "CONNECTION TIMED OUT", "CONNECT TIMEOUT", "CONNECTIONERROR", "DNS",
        ],
    ),
    (
        catalog.HIGH_PUBLISH_LATENCY,
        [
            "_MSG_TIMED_OUT", "_TIMED_OUT", "REQUEST TIMEOUT", "REQUEST_TIMED_OUT",
            "DELIVERY TIMEOUT", "TIMED OUT IN QUEUE", "READ TIMED OUT", "REQUESTS.EXCEPTIONS.TIMEOUT",
        ],
    ),
    (
        catalog.OUT_OF_MEMORY,
        ["MEMORYERROR", "OUTOFMEMORY", "CANNOT ALLOCATE MEMORY", "EXIT CODE 137", "OOMKILLED"],
    ),
    (
        catalog.FRED_AUDIT_STORE_FAILURE,
        [
            "RESOURCENOTFOUNDEXCEPTION", "PROVISIONEDTHROUGHPUTEXCEEDED",
            "CONDITIONALCHECKFAILED", "DYNAMODB", "INSERT FAILED",
        ],
    ),
    (
        catalog.BDP_READ_FAILURE,
        ["NOSUCHKEY", "NOSUCHBUCKET", "PATH NOT FOUND", "FILE NOT FOUND", "FILENOTFOUNDERROR"],
    ),
    (
        catalog.BDP_WRITE_FAILURE,
        ["ACCESSDENIED", "ACCESS DENIED", "KMS", "BUCKET POLICY"],
    ),
)


@dataclass(frozen=True)
class Classification:
    scenario: Scenario
    raw_error: str
    normalized: str
    operation: Optional[str] = None
    topic: Optional[str] = None
    kafka_error_name: Optional[str] = None
    kafka_error_code: Optional[Any] = None
    context: Optional[Dict[str, Any]] = None

    @property
    def exit_code(self) -> int:
        return self.scenario.exit_code

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scenario": self.scenario.key,
            "layer": self.scenario.layer.value,
            "error_type": self.scenario.error_type,
            "severity": self.scenario.severity.value,
            "handling": self.scenario.handling.value,
            "retryable": self.scenario.retryable,
            "producer_fix_required": self.scenario.producer_fix_required,
            "incident_owner": self.scenario.incident_owner.value,
            "inform": [o.value for o in self.scenario.inform],
            "exit_code": self.scenario.exit_code,
            "operation": self.operation,
            "topic": self.topic,
            "kafka_error_name": self.kafka_error_name,
            "kafka_error_code": self.kafka_error_code,
            "raw_error": self.raw_error[:2000],
            "context": self.context or {},
        }


def _safe_attr(obj: Any, name: str) -> Optional[str]:
    try:
        attr = getattr(obj, name, None)
        value = attr() if callable(attr) else attr
        return None if value is None else str(value)
    except Exception:  # pragma: no cover
        return None


def normalize(error: Any) -> str:
    if error is None:
        return ""

    parts: List[str] = []
    try:
        parts.append(str(error))
    except Exception:  # pragma: no cover
        parts.append(repr(error))

    for attr in ("name", "code", "str"):
        value = _safe_attr(error, attr)
        if value:
            parts.append(value)

    cause = getattr(error, "cause", None) or getattr(error, "__cause__", None)
    if cause is not None and cause is not error:
        try:
            parts.append(str(cause))
        except Exception:  # pragma: no cover
            pass

    return " | ".join(parts).upper()


def _matches(haystack: str, needles: List[str]) -> bool:
    compact = haystack.replace("_", "").replace(" ", "").replace("-", "")
    for needle in needles:
        upper = needle.upper()
        if upper in haystack:
            return True
        if upper.replace("_", "").replace(" ", "").replace("-", "") in compact:
            return True
    return False


def classify(
    error: Any,
    *,
    operation: Optional[str] = None,
    topic: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Classification:
    if isinstance(error, (ConnectorError, RecordRejected)):
        merged = dict(getattr(error, "context", None) or getattr(error, "detail", None) or {})
        merged.update(context or {})
        return Classification(
            scenario=error.scenario,
            raw_error=str(error),
            normalized=normalize(error),
            operation=operation,
            topic=topic,
            context=merged,
        )

    from utility.connector_utility import SourceAccessError

    if isinstance(error, SourceAccessError):
        scenario = (
            catalog.BDP_WRITE_FAILURE if error.operation == "write" else catalog.BDP_READ_FAILURE
        )
        return Classification(
            scenario=scenario,
            raw_error=str(error),
            normalized=normalize(error),
            operation=operation or f"s3_{error.operation}",
            topic=topic,
            context={**(context or {}), "path": error.path},
        )

    if isinstance(error, MemoryError):
        return Classification(
            scenario=catalog.OUT_OF_MEMORY,
            raw_error=str(error) or "MemoryError",
            normalized="MEMORYERROR",
            operation=operation,
            topic=topic,
            context=context,
        )

    normalized = normalize(error)
    scenario = catalog.UNKNOWN
    for candidate, patterns in _PATTERNS:
        if _matches(normalized, patterns):
            scenario = candidate
            break
    else:
        for candidate in catalog.SCENARIOS.values():
            if candidate.typical_errors and _matches(normalized, candidate.typical_errors):
                scenario = candidate
                break

    return Classification(
        scenario=scenario,
        raw_error=str(error) if error is not None else "",
        normalized=normalized,
        operation=operation,
        topic=topic,
        kafka_error_name=_safe_attr(error, "name"),
        kafka_error_code=_safe_attr(error, "code"),
        context=context,
    )
