"""Readiness checks run before a single record is read or published.

The POC readiness assessment lists the things that were unproven for the AWS to
on-prem BSP path: DNS from the AWS runtime, TCP on 9095/8095, BAM token
acquisition, registry access, cluster metadata, then publish. Those are exactly
the checks here, in that order, so a broken environment is diagnosed by the
connector's own startup log instead of by a separate diagnostic exercise.

Ordering is cheapest-and-most-likely-first: a firewall rule that was never
implemented should fail in under a second, not after a full auth handshake.
Each check returns a result rather than raising, so the log shows every check's
outcome before the run aborts on the first blocking failure.
"""

from __future__ import annotations

import logging
import socket
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from utility import failure_catalog as catalog
from utility.failure_catalog import Scenario
from utility.error_classifier import PreflightError

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    name: str
    passed: bool
    duration_ms: float
    detail: str = ""
    scenario: Optional[Scenario] = None
    blocking: bool = True
    context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check": self.name,
            "passed": self.passed,
            "duration_ms": round(self.duration_ms, 1),
            "detail": self.detail,
            "scenario": self.scenario.key if self.scenario else None,
            "blocking": self.blocking,
            **self.context,
        }


class PreflightReport:
    def __init__(self) -> None:
        self.results: List[CheckResult] = []

    def add(self, result: CheckResult) -> CheckResult:
        self.results.append(result)
        logger.log(
            logging.INFO if result.passed else logging.ERROR,
            "Preflight %s: %s",
            result.name,
            "ok" if result.passed else result.detail,
            extra=result.to_dict(),
        )
        return result

    @property
    def failures(self) -> List[CheckResult]:
        return [r for r in self.results if not r.passed and r.blocking]

    @property
    def passed(self) -> bool:
        return not self.failures

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "checks": [r.to_dict() for r in self.results],
        }

    def raise_if_failed(self) -> None:
        if self.passed:
            return

        first = self.failures[0]
        raise PreflightError(
            f"Preflight check '{first.name}' failed: {first.detail}",
            first.scenario or catalog.UNKNOWN,
            context={"preflight": self.to_dict()},
        )


def _timed(fn: Callable[[], Tuple[bool, str, Dict[str, Any]]]) -> Tuple[bool, str, Dict[str, Any], float]:
    start = time.perf_counter()
    try:
        ok, detail, context = fn()
    except Exception as exc:  # a check must never crash the run itself
        ok, detail, context = False, f"{type(exc).__name__}: {exc}", {}
    return ok, detail, context, (time.perf_counter() - start) * 1000.0


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def parse_bootstrap_servers(bootstrap: str) -> List[Tuple[str, int]]:
    endpoints: List[Tuple[str, int]] = []
    for entry in str(bootstrap).split(","):
        entry = entry.strip()
        if not entry:
            continue
        host, _, port = entry.rpartition(":")
        endpoints.append((host or entry, int(port) if port.isdigit() else 9092))
    return endpoints


def parse_url_endpoint(url: str) -> Tuple[str, int]:
    parsed = urlparse(url)
    default_port = 443 if parsed.scheme == "https" else 80
    return parsed.hostname or url, parsed.port or default_port


def check_dns(report: PreflightReport, endpoints: List[Tuple[str, int]], *, label: str) -> None:
    for host, _port in endpoints:
        def resolve(host: str = host) -> Tuple[bool, str, Dict[str, Any]]:
            addresses = sorted({info[4][0] for info in socket.getaddrinfo(host, None)})
            return True, "", {"host": host, "addresses": addresses}

        ok, detail, context, ms = _timed(resolve)
        report.add(
            CheckResult(
                name=f"dns:{label}:{host}",
                passed=ok,
                duration_ms=ms,
                detail=detail or f"resolved {context.get('addresses')}",
                scenario=None if ok else catalog.NETWORK_FAILURE,
                context=context,
            )
        )


def check_tcp(
    report: PreflightReport,
    endpoints: List[Tuple[str, int]],
    *,
    label: str,
    timeout: int,
    require_all: bool = False,
) -> None:
    """Open a socket to each endpoint.

    ``require_all`` is False for Kafka bootstrap servers: one reachable broker is
    enough to bootstrap a cluster, and failing because a single broker is down
    would be a self-inflicted outage. Unreachable brokers are still reported.
    """
    reachable = 0

    for host, port in endpoints:
        def connect(host: str = host, port: int = port) -> Tuple[bool, str, Dict[str, Any]]:
            with socket.create_connection((host, port), timeout=timeout):
                return True, "", {"host": host, "port": port}

        ok, detail, context, ms = _timed(connect)
        reachable += 1 if ok else 0
        report.add(
            CheckResult(
                name=f"tcp:{label}:{host}:{port}",
                passed=ok,
                duration_ms=ms,
                detail=detail or f"connected to {host}:{port}",
                scenario=None if ok else catalog.NETWORK_FAILURE,
                blocking=require_all,
                context={"host": host, "port": port},
            )
        )

    if not require_all:
        report.add(
            CheckResult(
                name=f"tcp:{label}:quorum",
                passed=reachable > 0,
                duration_ms=0.0,
                detail=f"{reachable}/{len(endpoints)} endpoints reachable",
                scenario=None if reachable else catalog.NETWORK_FAILURE,
                context={"reachable": reachable, "total": len(endpoints)},
            )
        )


def check_source(report: PreflightReport, *, table: str, probe: Callable[[], Any]) -> None:
    """Confirm the trigger table is visible to this role before authenticating to BSP."""

    def run() -> Tuple[bool, str, Dict[str, Any]]:
        probe()
        return True, "", {"source_table": table}

    ok, detail, context, ms = _timed(run)
    report.add(
        CheckResult(
            name="source:readable",
            passed=ok,
            duration_ms=ms,
            detail=detail or "readable",
            scenario=None if ok else catalog.BDP_READ_FAILURE,
            context=context or {"source_table": table},
        )
    )


def check_authentication(report: PreflightReport, *, acquire: Callable[[], Dict[str, Any]]) -> None:
    def run() -> Tuple[bool, str, Dict[str, Any]]:
        return True, "", acquire()

    ok, detail, context, ms = _timed(run)
    report.add(
        CheckResult(
            name="auth:bam_token",
            passed=ok,
            duration_ms=ms,
            detail=detail or "token acquired",
            scenario=None if ok else catalog.AUTHENTICATION_FAILURE,
            context=context,
        )
    )


def check_schema_registry(report: PreflightReport, *, resolve: Callable[[], Dict[str, Any]]) -> None:
    def run() -> Tuple[bool, str, Dict[str, Any]]:
        return True, "", resolve()

    ok, detail, context, ms = _timed(run)

    # A compatibility finding is a schema fault, not a registry outage; route it
    # so the alert names the right team.
    scenario = None
    if not ok:
        scenario = (
            catalog.SCHEMA_VALIDATION_FAILURE
            if "compat" in detail.lower() or "schema" in detail.lower()
            else catalog.SCHEMA_REGISTRY_UNAVAILABLE
        )

    report.add(
        CheckResult(
            name="schema_registry:subject",
            passed=ok,
            duration_ms=ms,
            detail=detail or f"subject resolved (id={context.get('schema_id')})",
            scenario=scenario,
            context=context,
        )
    )


def check_topic_metadata(
    report: PreflightReport,
    *,
    fetch: Callable[[str], Dict[str, Any]],
    topics: List[str],
) -> None:
    """Fetch metadata with the real producer principal.

    This is the check that separates 'topic does not exist' from 'no ACL for
    this principal' - the two most common onboarding failures, which look
    identical from a publish timeout.
    """
    for topic in topics:
        def run(topic: str = topic) -> Tuple[bool, str, Dict[str, Any]]:
            return True, "", fetch(topic)

        ok, detail, context, ms = _timed(run)

        scenario = None
        if not ok:
            upper = detail.upper()
            if "AUTH" in upper or "403" in upper:
                scenario = catalog.AUTHORISATION_FAILURE
            elif "UNKNOWN_TOPIC" in upper.replace(" ", "_"):
                scenario = catalog.TOPIC_UNAVAILABLE
            else:
                scenario = catalog.BROKER_UNAVAILABLE

        report.add(
            CheckResult(
                name=f"kafka:metadata:{topic}",
                passed=ok,
                duration_ms=ms,
                detail=detail or f"{context.get('partitions')} partitions",
                scenario=scenario,
                context={"topic": topic, **context},
            )
        )
