"""The upstream reconciliation gate.

Before any work, this decides whether upstream has anything worth running for.
The Databricks recon job appends one row per model run to a recon table that
Athena reads::

    target_table_name   `sit_cds_snsvc0080860_prepared_db`.bdb_ifc_synthetic_data_test.BDP_Corp_IFC_Trigger_9
    status              SUCCESS | RECON_FAILED | FAILED
    source_count, target_count, error_record_count
    last_modified_ts    when the row was written (UTC)
    idempotency_key, batch_id, model_name, job_run_id, env, dataproduct_name,
    last_modified_by

A rerun appends a new row rather than updating the old one, so the newest row
for the run's trigger - matched on ``target_table_name`` - is upstream's current
verdict. Only that row is read.

What the newest row decides, each mapped onto the agreed failure catalogue so
the ECS stopped-task record alone tells RTB which one fired:

* no row for the trigger - upstream data was never received;
* the row is not from the current month - upstream has not processed this month
  yet, and publishing would republish last month;
* ``FAILED`` - the dbt model itself did not run;
* ``RECON_FAILED`` - the model ran but upstream's reconciliation failed;
* ``SUCCESS`` with counts that disagree - upstream's own definition of a failed
  reconciliation, reported as SUCCESS by mistake;
* ``SUCCESS`` with both counts zero - a genuine month with no data. Not a
  failure: exit 0, no alert, and the caller announces a zero-message batch to
  TBB and closes the month;
* ``SUCCESS`` with matching, non-zero counts - the run proceeds and publishes.

Every blocking reason carries the row's own details (model, job run, batch,
counts, timestamp), because that reason is what the alert says.

Stopping is the safe direction: a recon table that cannot be queried, or a row
that cannot be understood, blocks the run rather than letting it proceed on an
assumption. A status outside the three is untrusted, never a green light.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from itertools import islice
from typing import Any, Callable, Dict, List, Optional, Tuple

from botocore.exceptions import BotoCoreError, ClientError

from utility import athena_query
from utility import failure_catalog as catalog
from utility.connector_config import athena_table
from utility.connector_utility import SourceAccessError

logger = logging.getLogger(__name__)

#: ``2026-09-30T14:30:22.123``, or Athena's ``2026-09-30 14:30:22.123``.
_LAST_MODIFIED_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
)

#: The one status that lets a run proceed.
STATUS_SUCCESS = "SUCCESS"
#: The model ran, but upstream's reconciliation did not balance.
STATUS_RECON_FAILED = "RECON_FAILED"
#: The dbt model itself did not run.
STATUS_FAILED = "FAILED"

#: Upstream reconciled SUCCESS with zero records: nothing to publish, and that
#: is a delivered month, not a failure.
OUTCOME_NO_DATA = "NO_DATA_THIS_MONTH"

REQUIRED_FIELDS = ("last_modified_ts", "status")

#: The row details quoted in every reason, in this order, when present.
_DETAIL_FIELDS = (
    "status",
    "source_count",
    "target_count",
    "error_record_count",
    "model_name",
    "job_run_id",
    "batch_id",
    "last_modified_ts",
)


@dataclass
class ReconDecision:
    proceed: bool
    outcome: str
    exit_code: int
    reason: str
    scenario_key: Optional[str] = None
    document: Optional[Dict[str, Any]] = None
    table: Optional[str] = None
    target: Optional[str] = None
    query_execution_id: Optional[str] = None

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
            "recon_table": self.table,
            "recon_target": self.target,
            "query_execution_id": self.query_execution_id,
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
    """The row's ``last_modified_ts`` as a datetime, or ``None`` if unreadable.

    Athena returns a ``timestamp`` column already typed; text is accepted too.
    A trailing ``Z`` or an offset is tolerated: the value is UTC and only its
    month is read.
    """
    if isinstance(value, datetime):
        return value
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
    """Counts arrive as numbers (bigint columns) or, defensively, as strings."""
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


def normalise_target(name: str) -> str:
    """``target_table_name`` compared without backticks or case.

    Databricks names are case-insensitive and the catalog part is quoted with
    backticks because it holds hyphens, so a row written as
    ```sit_cds_snsvc0080860_prepared_db`.bdb_ifc_synthetic_data_test.BDP_Corp_IFC_Trigger_9`` and a config
    value written without the backticks still match.
    """
    return str(name).replace("`", "").strip().lower()


def recon_query(table: str, target: str) -> Tuple[str, List[str]]:
    """The SQL for the newest row naming ``target``, and its parameters."""
    sql = (
        f"SELECT * FROM {athena_table(table)} "
        "WHERE lower(replace(target_table_name, '`', '')) = ? "
        "ORDER BY last_modified_ts DESC "
        "LIMIT 1"
    )
    return sql, [athena_query.string_literal(normalise_target(target))]


def _json_safe(row: Dict[str, Any]) -> Dict[str, Any]:
    """The row as it goes into the summary, manifest and alert: JSON values only."""
    return {
        key: value.isoformat() if isinstance(value, (date, datetime)) else value
        for key, value in row.items()
    }


def _details(row: Dict[str, Any]) -> str:
    parts = [f"{field}={row[field]}" for field in _DETAIL_FIELDS if row.get(field) is not None]
    return f"({', '.join(parts)})"


def evaluate(
    settings,
    athena,
    *,
    execution_month: str,
    client: Any = None,
    sleep: Callable[[float], None] = time.sleep,
) -> ReconDecision:
    """Decide from the recon table whether upstream has produced this month's data.

    ``settings`` is ``ReconSettings`` with ``table`` and ``target_table``
    resolved; ``athena`` is ``source.athena`` (workgroup, catalog, result
    location, polling). ``execution_month`` is ``YYYY-MM`` - the month the
    newest row's ``last_modified_ts`` has to fall in. The entry point passes the
    current month, not ``IFC_RUN__MONTH``: upstream writes its recon row when it
    runs, even when an earlier month is being reprocessed.
    """
    table, target = settings.table, settings.target_table
    if not table or not target:
        raise ValueError("recon.table and the run's recon target must be resolved before evaluating")

    common: Dict[str, Any] = {"table": table, "target": target}

    try:
        client = client or athena_query.athena_client()
        sql, parameters = recon_query(table, target)
        query_id = athena_query.start(client, athena, sql, parameters, label=table)
        common["query_execution_id"] = query_id
        athena_query.wait(client, athena, query_id, label=table, sleep=sleep)
        found = list(islice(athena_query.rows(client, query_id), 1))
    except (SourceAccessError, ClientError, BotoCoreError) as exc:
        return _blocked(
            "UPSTREAM_RECON_UNREADABLE",
            catalog.BDP_READ_FAILURE,
            f"The reconciliation table {table} could not be read: {exc}",
            **common,
        )

    if not found:
        return _blocked(
            "UPSTREAM_DATA_NOT_RECEIVED",
            catalog.TED_MISSING_SOURCE_DATA,
            f"No reconciliation row in {table} for {target}: upstream data not received "
            f"for {execution_month}",
            **common,
        )

    raw = found[0]
    document = _json_safe(raw)
    common["document"] = document
    details = _details(document)

    missing = [f for f in REQUIRED_FIELDS if raw.get(f) in (None, "")]
    if missing:
        return _blocked(
            "UPSTREAM_RECON_UNREADABLE",
            catalog.SCHEMA_VALIDATION_FAILURE,
            f"The newest reconciliation row for {target} is missing {missing} {details}",
            **common,
        )

    # 1. Is it this month's?
    last_modified = parse_last_modified(raw.get("last_modified_ts"))
    if last_modified is None:
        return _blocked(
            "UPSTREAM_RECON_UNREADABLE",
            catalog.SCHEMA_VALIDATION_FAILURE,
            f"The newest reconciliation row for {target} has an unreadable last_modified_ts "
            f"{raw.get('last_modified_ts')!r} {details}",
            **common,
        )

    recon_month = last_modified.strftime("%Y-%m")
    if recon_month != execution_month:
        return _blocked(
            "UPSTREAM_PROCESSING_NOT_DONE",
            catalog.TED_MISSING_SOURCE_DATA,
            f"Upstream processing is not done for {execution_month}: the newest "
            f"reconciliation row for {target} is from {recon_month} {details}",
            **common,
        )

    # 2. Did the model run, and did upstream's own reconciliation pass?
    status = str(raw.get("status", "")).strip().upper()

    if status == STATUS_FAILED:
        return _blocked(
            "UPSTREAM_MODEL_FAILED",
            catalog.TED_JOB_FAILURE,
            f"Upstream job failed for {execution_month}: the dbt model for {target} "
            f"did not run {details}",
            **common,
        )

    if status == STATUS_RECON_FAILED:
        return _blocked(
            "UPSTREAM_JOB_FAILED",
            catalog.TED_JOB_FAILURE,
            f"Upstream job failed for {execution_month}: upstream reconciliation for "
            f"{target} failed {details}",
            **common,
        )

    if status != STATUS_SUCCESS:
        # The contract is exactly three statuses. Anything else means the row
        # does not match the contract it was read under, so it is untrusted
        # rather than a fourth outcome to interpret - there is no state in which
        # an unknown status is a green light.
        return _blocked(
            "UPSTREAM_RECON_UNREADABLE",
            catalog.SCHEMA_VALIDATION_FAILURE,
            f"The newest reconciliation row for {target} has status {status!r}; the "
            f"contract is {STATUS_SUCCESS}, {STATUS_RECON_FAILED} or {STATUS_FAILED} {details}",
            **common,
        )

    # 3. It succeeded - but did it produce anything?
    source_count = _as_int(raw.get("source_count"))
    target_count = _as_int(raw.get("target_count"))
    if source_count is None or target_count is None:
        return _blocked(
            "UPSTREAM_RECON_UNREADABLE",
            catalog.SCHEMA_VALIDATION_FAILURE,
            f"The newest reconciliation row for {target} has non-numeric counts {details}",
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
                f"{target} successfully with zero source and target records"
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
            f"should have reported {STATUS_RECON_FAILED} {details}",
            **common,
        )

    error_count = _as_int(raw.get("error_record_count")) or 0
    if error_count:
        logger.warning(
            "Upstream reports SUCCESS with error records",
            extra={"recon_target": target, "error_record_count": error_count},
        )

    return ReconDecision(
        proceed=True,
        outcome="PROCEED",
        exit_code=catalog.EXIT_OK,
        reason=(
            f"Upstream reconciled {target} for {execution_month}: {status}, "
            f"source_count={source_count}, target_count={target_count}"
        ),
        **common,
    )
