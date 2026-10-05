"""The upstream reconciliation gate.

Before the run gate decides whether this month is due, this decides whether the
upstream side has anything worth running for. TED writes one recon document per
trigger per month::

    .../ifc-bdp-audit-recon-json/trigger8/SEPTEMBER_2026/
        BDP_Corp_Trigger_8_recon_20260930_143022_123456.json

and the newest one in the month's folder is read - the same rule the source
extracts follow, ordered by the timestamp parsed out of the filename rather than
by the name, so a naming change cannot silently select a stale document. Each
document holds a single record::

    {"target_table_name": "...", "last_modified_ts": "2026-09-30T14:30:22.123",
     "status": "SUCCESS", "source_count": 412, "target_count": 412,
     "error_record_count": 0}

``status`` is strictly ``SUCCESS`` or ``RECON_FAILED``; an upstream job that
failed outright writes no document at all, so an empty folder is its own signal.

Five ways the gate stops the run. Four are failures, each mapped onto the agreed
failure catalogue so the ECS stopped-task record alone tells RTB which one fired;
the fifth - a genuine empty month - is a successful outcome:

* the folder or the document is absent - upstream data was never received, which
  is also what an outright upstream job failure looks like;
* ``last_modified_ts`` is not in the execution month - upstream has not
  processed this month yet, and publishing would republish last month;
* ``status`` is ``RECON_FAILED`` - the upstream job failed;
* ``status`` is ``SUCCESS`` but the counts disagree - upstream's own definition
  of a failed reconciliation, reported as SUCCESS by mistake;
* ``status`` is ``SUCCESS`` and both counts are zero - a genuine month with no
  data, so the source is never even looked at. Not a failure: exit 0, no alert,
  and the caller announces a zero-message batch to TBB and closes the month.

Stopping is the safe direction: a document that cannot be read, parsed or
understood blocks the run rather than letting it proceed on an assumption, and
because the contract is exactly two statuses, a third one is treated as an
untrusted document rather than a third outcome to interpret.
"""

from __future__ import annotations

import json
import logging
import posixpath
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from utility import failure_catalog as catalog
from utility.connector_config import expand_date_tokens
from utility.connector_utility import (
    SourceAccessError,
    iter_object_paths,
    read_text,
)

logger = logging.getLogger(__name__)


def parse_filename_timestamp(name: str, *, pattern: str, fmt: str) -> Optional[datetime]:
    """Pull the recon document's timestamp out of its filename, or ``None``.

    ``None`` is not an error: a hand-placed file, or one TED names differently,
    simply cannot be ordered against the others and loses to any file that can.
    """
    match = re.search(pattern, name)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), fmt)
    except ValueError:
        # The pattern matched but the value is not a real date - a 13th month,
        # or a day-first string read as month-first. Not fatal: it just cannot
        # order this file.
        return None

#: ``2026-09-30T14:30:22.123`` - milliseconds, three digits.
_LAST_MODIFIED_FORMATS = ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S")

#: The one status that lets a run proceed.
STATUS_SUCCESS = "SUCCESS"
#: The status TED writes when its own reconciliation did not balance.
STATUS_RECON_FAILED = "RECON_FAILED"

#: Upstream reconciled SUCCESS with zero records: nothing to publish, and that
#: is a delivered month, not a failure.
OUTCOME_NO_DATA = "NO_DATA_THIS_MONTH"

REQUIRED_FIELDS = ("last_modified_ts", "status")


@dataclass(frozen=True)
class ReconDecision:
    """Whether to proceed, and everything the manifest and the alert need."""

    proceed: bool
    outcome: str
    exit_code: int
    reason: str
    scenario_key: Optional[str] = None
    document: Optional[Dict[str, Any]] = None
    recon_object: Optional[str] = None
    folder: Optional[str] = None

    @property
    def no_data(self) -> bool:
        """A genuine empty month: the run stops, but successfully."""
        return self.outcome == OUTCOME_NO_DATA

    @property
    def blocking_scenario(self) -> catalog.Scenario:
        """The catalogue scenario behind a block.

        ``scenario_key`` is optional because a *proceeding* decision has none,
        which a caller on the blocking path can see is impossible but a type
        checker cannot. This narrows it in one place instead of at every call
        site, and falls back to ``UNKNOWN`` rather than raising: a gate that
        blocked the run must still be able to report why.
        """
        if not self.scenario_key:
            return catalog.UNKNOWN
        return catalog.get(self.scenario_key)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "proceed": self.proceed,
            "outcome": self.outcome,
            "exit_code": self.exit_code,
            "reason": self.reason,
            "scenario": self.scenario_key,
            "recon_object": self.recon_object,
            "recon_folder": self.folder,
            "recon": self.document,
        }


def _blocked(outcome: str, scenario, reason: str, **extra) -> ReconDecision:
    return ReconDecision(
        proceed=False,
        outcome=outcome,
        exit_code=scenario.exit_code,
        reason=reason,
        scenario_key=scenario.key,
        **extra,
    )


def parse_last_modified(value: Any) -> Optional[datetime]:
    """``2026-09-30T14:30:22.123`` -> datetime, or ``None`` if unreadable.

    A trailing ``Z`` or an offset is tolerated: the value names an instant in
    upstream's own clock and only its month is read, so the offset cannot change
    which month it belongs to by more than the hours either side of midnight -
    and a month boundary that close is upstream's to get right, not ours to
    guess at.
    """
    if not isinstance(value, str) or not value.strip():
        return None

    text = value.strip()
    text = re.sub(r"(Z|[+-]\d{2}:?\d{2})$", "", text).strip()

    for fmt in _LAST_MODIFIED_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _as_int(value: Any) -> Optional[int]:
    """Counts arrive as numbers or as strings, depending on the writer."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


class ReconSource:
    """Finds and reads the newest recon document for a trigger and month."""

    def __init__(self, settings, *, run_date: Optional[date] = None):
        self._settings = settings
        self._run_date = run_date

    @property
    def folder(self) -> str:
        return expand_date_tokens(self._settings.resolved_path, self._run_date)

    def _rank(self, path: str) -> Tuple[int, datetime, str]:
        stamp = parse_filename_timestamp(
            posixpath.basename(path.rstrip("/")),
            pattern=self._settings.filename_timestamp_pattern,
            fmt=self._settings.filename_timestamp_format,
        )
        if stamp is None:
            return (0, datetime.min, path)
        return (1, stamp, path)

    def latest_object(self) -> Optional[str]:
        """The newest recon document, or ``None`` when the folder is empty.

        A missing folder is not an error here: it is the 'upstream data not
        received' case, which the caller reports with its own exit code.
        """
        try:
            found: List[str] = list(
                iter_object_paths(self.folder, self._settings.file_suffixes)
            )
        except SourceAccessError:
            logger.info(
                "No reconciliation folder for this month",
                extra={"recon_folder": self.folder},
            )
            return None

        if not found:
            return None

        latest = max(found, key=self._rank)
        if len(found) > 1:
            logger.info(
                "Selected the newest reconciliation document",
                extra={"selected": latest, "superseded": len(found) - 1},
            )
        return latest


def evaluate(
    settings,
    *,
    execution_month: str,
    run_date: Optional[date] = None,
) -> ReconDecision:
    """Decide whether upstream has produced this month's data.

    ``execution_month`` is ``YYYY-MM`` - the month the run gate keys on, and the
    month the recon document's ``last_modified_ts`` has to fall in.
    """
    source = ReconSource(settings, run_date=run_date)
    folder = source.folder

    path = source.latest_object()
    if path is None:
        return _blocked(
            "UPSTREAM_DATA_NOT_RECEIVED",
            catalog.TED_MISSING_SOURCE_DATA,
            f"No reconciliation document under {folder}: upstream data not received "
            f"for {execution_month}",
            folder=folder,
        )

    try:
        body = read_text(path)
    except SourceAccessError as exc:
        return _blocked(
            "UPSTREAM_RECON_UNREADABLE",
            catalog.BDP_READ_FAILURE,
            f"Reconciliation document {path} could not be read: {exc}",
            recon_object=path,
            folder=folder,
        )

    try:
        document = json.loads(body)
    except ValueError as exc:
        return _blocked(
            "UPSTREAM_RECON_UNREADABLE",
            catalog.SCHEMA_VALIDATION_FAILURE,
            f"Reconciliation document {path} is not valid JSON: {exc}",
            recon_object=path,
            folder=folder,
        )

    # One record per document. A single-element array is accepted because a
    # Spark write produces one either way.
    if isinstance(document, list):
        if len(document) != 1:
            return _blocked(
                "UPSTREAM_RECON_UNREADABLE",
                catalog.SCHEMA_VALIDATION_FAILURE,
                f"Reconciliation document {path} holds {len(document)} records; "
                "exactly one is expected",
                recon_object=path,
                folder=folder,
            )
        document = document[0]

    if not isinstance(document, dict):
        return _blocked(
            "UPSTREAM_RECON_UNREADABLE",
            catalog.SCHEMA_VALIDATION_FAILURE,
            f"Reconciliation document {path} is not a JSON object",
            recon_object=path,
            folder=folder,
        )

    missing = [f for f in REQUIRED_FIELDS if not document.get(f)]
    if missing:
        return _blocked(
            "UPSTREAM_RECON_UNREADABLE",
            catalog.SCHEMA_VALIDATION_FAILURE,
            f"Reconciliation document {path} is missing {missing}",
            recon_object=path,
            document=document,
            folder=folder,
        )

    common = {"recon_object": path, "document": document, "folder": folder}

    # 1. Is it this month's?
    last_modified = parse_last_modified(document.get("last_modified_ts"))
    if last_modified is None:
        return _blocked(
            "UPSTREAM_RECON_UNREADABLE",
            catalog.SCHEMA_VALIDATION_FAILURE,
            f"Reconciliation document {path} has an unreadable last_modified_ts "
            f"{document.get('last_modified_ts')!r}; expected YYYY-MM-DDTHH:MM:SS.mmm",
            **common,
        )

    recon_month = last_modified.strftime("%Y-%m")
    if recon_month != execution_month:
        return _blocked(
            "UPSTREAM_PROCESSING_NOT_DONE",
            catalog.TED_MISSING_SOURCE_DATA,
            f"Upstream processing is not done for {execution_month}: the latest "
            f"reconciliation is from {recon_month} "
            f"(last_modified_ts {document.get('last_modified_ts')})",
            **common,
        )

    # 2. Did upstream's own reconciliation pass?
    counts = (
        f"(source_count={document.get('source_count')}, "
        f"target_count={document.get('target_count')}, "
        f"error_record_count={document.get('error_record_count')})"
    )
    status = str(document.get("status", "")).strip().upper()

    if status == STATUS_RECON_FAILED:
        return _blocked(
            "UPSTREAM_JOB_FAILED",
            catalog.TED_JOB_FAILURE,
            f"Upstream job failed for {execution_month}: upstream reconciliation "
            f"failed {counts}",
            **common,
        )

    if status != STATUS_SUCCESS:
        # The contract is exactly two statuses. Anything else means the document
        # does not match the contract it was read under, so it is untrusted
        # rather than a third outcome to interpret - there is no state in which
        # an unknown status is a green light.
        return _blocked(
            "UPSTREAM_RECON_UNREADABLE",
            catalog.SCHEMA_VALIDATION_FAILURE,
            f"Reconciliation document {path} has status {status!r}; the contract "
            f"is {STATUS_SUCCESS} or {STATUS_RECON_FAILED} {counts}",
            **common,
        )

    # 3. It succeeded - but did it produce anything?
    source_count = _as_int(document.get("source_count"))
    target_count = _as_int(document.get("target_count"))
    if source_count is None or target_count is None:
        return _blocked(
            "UPSTREAM_RECON_UNREADABLE",
            catalog.SCHEMA_VALIDATION_FAILURE,
            f"Reconciliation document {path} has non-numeric counts "
            f"(source_count={document.get('source_count')!r}, "
            f"target_count={document.get('target_count')!r})",
            **common,
        )

    if source_count == 0 and target_count == 0:
        # Stops the run - there is nothing to publish - but it is not a
        # failure: no catalogue scenario, exit 0.
        return ReconDecision(
            proceed=False,
            outcome=OUTCOME_NO_DATA,
            exit_code=catalog.EXIT_OK,
            reason=(
                f"No data to process for {execution_month}: upstream reconciled "
                "successfully with zero source and target records"
            ),
            **common,
        )

    # Counts that disagree are upstream's own definition of a failed
    # reconciliation, so a SUCCESS carrying them is a contradiction: upstream
    # should have written RECON_FAILED and did not. Stop rather than publish a
    # month upstream cannot account for.
    if source_count != target_count:
        return _blocked(
            "UPSTREAM_COUNT_MISMATCH",
            catalog.TED_JOB_FAILURE,
            f"Upstream issue for {execution_month}: status is {STATUS_SUCCESS} but "
            f"source_count {source_count} != target_count {target_count}; upstream "
            f"should have reported {STATUS_RECON_FAILED} {counts}",
            **common,
        )

    error_count = _as_int(document.get("error_record_count")) or 0
    if error_count:
        logger.warning(
            "Upstream reports SUCCESS with error records",
            extra={"recon_object": path, "error_record_count": error_count},
        )

    return ReconDecision(
        proceed=True,
        outcome="PROCEED",
        exit_code=catalog.EXIT_OK,
        reason=(
            f"Upstream reconciled for {execution_month}: {status}, "
            f"source_count={source_count}, target_count={target_count}"
        ),
        document=document,
        recon_object=path,
        folder=folder,
    )
