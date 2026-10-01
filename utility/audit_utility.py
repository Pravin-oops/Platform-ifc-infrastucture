"""Run manifest, reconciliation and quarantine.

The reconciliation control asks a simple question - can you prove every trigger
event that was detected was either published or accounted for? The manifest
answers it with an identity that must hold for every run::

    records read = published + quarantined + parse failures

If it does not balance, the run fails even when every individual Kafka publish
succeeded, because an unexplained gap is exactly the condition the control
exists to catch. That is the difference between a producer that reports success
and one that can evidence it.
"""

from __future__ import annotations

import json
import logging
import socket
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from utility import failure_catalog as catalog
from utility.error_classifier import Classification
from utility.connector_utility import join_path, write_bytes, write_json
from utility.connector_config import AuditSettings

logger = logging.getLogger(__name__)


def new_run_id() -> str:
    """Sortable, unique run identifier: ``20260610T021500Z-ab12cd34``."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


def ecs_task_identity() -> Dict[str, Optional[str]]:
    """Best-effort ECS task identity from the metadata endpoint's environment.

    Read from the environment rather than by calling the metadata endpoint: the
    call can hang when the network path is the thing that is broken, and this is
    used on the failure path.
    """
    import os

    return {
        "hostname": socket.gethostname(),
        "task_arn": os.environ.get("ECS_TASK_ARN"),
        "container_name": os.environ.get("ECS_CONTAINER_NAME"),
        "cluster": os.environ.get("ECS_CLUSTER"),
        "image_tag": os.environ.get("IFC_IMAGE_TAG"),
    }


@dataclass
class RunCounters:
    """Every record's fate, counted exactly once."""

    objects_read: int = 0
    records_parsed: int = 0
    parse_failures: int = 0
    quarantined: int = 0
    published: int = 0
    acked: int = 0
    delivery_failed: int = 0
    unflushed: int = 0

    def to_dict(self) -> Dict[str, int]:
        return asdict(self)


@dataclass
class ReconciliationResult:
    balanced: bool
    expected: int
    accounted: int
    findings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "balanced": self.balanced,
            "expected": self.expected,
            "accounted": self.accounted,
            "findings": self.findings,
        }


def reconcile(counters: RunCounters) -> ReconciliationResult:
    findings: List[str] = []

    # Parse failures are counted separately from parsed records, so the total
    # the run is answerable for is everything it read.
    expected = counters.records_parsed + counters.parse_failures
    accounted = (
        counters.published
        + counters.quarantined
        + counters.parse_failures
    )

    if accounted != expected:
        findings.append(
            f"record accounting does not balance: read {expected}, accounted for {accounted} "
            f"(published={counters.published}, quarantined={counters.quarantined}, "
            f"parse_failures={counters.parse_failures})"
        )

    if counters.acked != counters.published:
        findings.append(
            f"{counters.published - counters.acked} of {counters.published} published messages "
            "were not acknowledged by the broker"
        )

    if counters.delivery_failed:
        findings.append(f"{counters.delivery_failed} messages failed delivery")

    if counters.unflushed:
        findings.append(
            f"{counters.unflushed} messages were still queued when the flush timed out; "
            "their delivery is uncertain and must be confirmed against topic offsets"
        )


    return ReconciliationResult(
        balanced=not findings, expected=expected, accounted=accounted, findings=findings
    )


class AuditWriter:
    """Writes quarantine objects, serialised payloads and the run manifest."""

    def __init__(self, settings: AuditSettings, *, run_id: str, environment: str):
        self._settings = settings
        self._run_id = run_id
        self._environment = environment
        self._quarantine_keys: List[str] = []

    @property
    def enabled(self) -> bool:
        return bool(self._settings.bucket)

    @property
    def quarantine_keys(self) -> List[str]:
        return list(self._quarantine_keys)

    def _base(self, prefix: str) -> str:
        return f"{self._settings.root}/{prefix.strip('/')}"

    # -- quarantine --------------------------------------------------------

    def quarantine(
        self,
        *,
        trigger_id: Optional[str],
        reason: str,
        scenario_key: str,
        detail: Dict[str, Any],
        record: Optional[Dict[str, Any]] = None,
        raw: Optional[str] = None,
    ) -> Optional[str]:
        """Persist a rejected record with enough context to fix and replay it."""
        if not self.enabled:
            logger.warning(
                "Quarantine requested but audit.bucket is not configured; the record exists only in the log",
                extra={"trigger_id": trigger_id, "scenario": scenario_key, "reason": reason},
            )
            return None

        name = (trigger_id or f"unidentified-{uuid.uuid4().hex[:8]}").replace("/", "_").replace(":", "_")
        key = join_path(
            self._base(self._settings.quarantine_prefix),
            f"run_id={self._run_id}",
            f"scenario={scenario_key}",
            f"{name}.json",
        )

        document = {
            "run_id": self._run_id,
            "environment": self._environment,
            "quarantined_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "trigger_id": trigger_id,
            "scenario": scenario_key,
            "reason": reason,
            "detail": detail,
            "record": record,
            "raw": raw,
        }

        try:
            write_json(key, document)
        except Exception:
            # Quarantine is best-effort by necessity: if S3 is the thing that is
            # broken, losing the quarantine object must not also lose the run.
            logger.exception("Failed to write quarantine object", extra={"key": key})
            return None

        self._quarantine_keys.append(key)
        logger.warning(
            "Record quarantined",
            extra={"trigger_id": trigger_id, "scenario": scenario_key, "quarantine_key": key},
        )
        return key

    # -- serialised payload evidence ---------------------------------------

    def write_payload(self, *, trigger_id: str, avro_bytes: bytes, event_date: str) -> Optional[str]:
        if not (self.enabled and self._settings.write_payloads):
            return None

        safe = trigger_id.replace("/", "_").replace(":", "_")
        key = join_path(
            self._base(self._settings.payload_prefix),
            f"event_date={event_date}",
            f"trigger_id={safe}",
            "payload.avro",
        )
        write_bytes(key, avro_bytes)
        return key

    # -- manifest ----------------------------------------------------------

    def write_manifest(self, manifest: Dict[str, Any]) -> Optional[str]:
        if not self.enabled:
            logger.info("No audit bucket configured; manifest emitted to the log only")
            logger.info("RUN_MANIFEST %s", json.dumps(manifest, default=str))
            return None

        key = join_path(
            self._base(self._settings.manifest_prefix),
            f"run_date={self._run_id[:8]}",
            f"{self._run_id}.json",
        )
        write_json(key, manifest)
        logger.info("Run manifest written", extra={"manifest_key": key})
        return key


def build_manifest(
    *,
    run_id: str,
    settings: Any,
    counters: RunCounters,
    reconciliation: ReconciliationResult,
    delivery_stats: Dict[str, Any],
    preflight: Optional[Dict[str, Any]],
    started_at: str,
    finished_at: str,
    duration_seconds: float,
    outcome: str,
    exit_code: int,
    classification: Optional[Classification] = None,
    source_objects: Optional[List[str]] = None,
    quarantine_keys: Optional[List[str]] = None,
    schema_id: Optional[int] = None,
    stage: str = "RUN",
    reason: Optional[str] = None,
    gate: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """The full record of one run, in the shape the reconciliation control reads.

    ``stage`` says how far the invocation got: ``RUN`` for one that reached the
    runner, or the gate / startup step that stopped it before then.
    """
    return {
        "run_id": run_id,
        "stage": stage,
        "outcome": outcome,
        "exit_code": exit_code,
        "reason": reason,
        "environment": settings.app.environment,
        "application": settings.app.name,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": round(duration_seconds, 3),
        "task": ecs_task_identity(),
        "configuration": {
            "trigger": settings.run.trigger,
            "source_path": settings.source.path,
            "topic": settings.kafka.topic,
            "schema_registry_mode": settings.schema_registry.mode,
            "schema_id": schema_id,
            "max_message_bytes": settings.kafka.max_message_bytes,
        },
        "counters": counters.to_dict(),
        "reconciliation": reconciliation.to_dict(),
        "delivery": delivery_stats,
        "preflight": preflight,
        "source_objects": source_objects or [],
        "quarantine_objects": quarantine_keys or [],
        "failure": classification.to_dict() if classification else None,
        "gate": gate,
        "catalogue_version": len(catalog.SCENARIOS),
    }


def write_invocation_manifest(
    settings: Any,
    *,
    stage: str,
    outcome: str,
    exit_code: int,
    started_at: str,
    duration_seconds: float,
    reason: Optional[str] = None,
    classification: Optional[Classification] = None,
    gate: Optional[Dict[str, Any]] = None,
) -> str:
    """Manifest for an invocation that stopped before the runner.

    The runner writes its own manifest; this covers the gate skips, the upstream
    recon block and startup failures, so every ECS invocation leaves a record.
    Best-effort: a failed write is logged and never changes the exit code.
    """
    run_id = new_run_id()
    counters = RunCounters()
    manifest = build_manifest(
        run_id=run_id,
        settings=settings,
        counters=counters,
        reconciliation=reconcile(counters),
        delivery_stats={},
        preflight=None,
        started_at=started_at,
        finished_at=datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        duration_seconds=duration_seconds,
        outcome=outcome,
        exit_code=exit_code,
        classification=classification,
        stage=stage,
        reason=reason,
        gate=gate,
    )
    try:
        AuditWriter(settings.audit, run_id=run_id, environment=settings.app.environment).write_manifest(manifest)
    except Exception:
        logger.exception("Manifest write failed; the invocation record exists in the log only")
    return run_id
