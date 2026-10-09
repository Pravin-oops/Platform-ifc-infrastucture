from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from itertools import islice
from typing import Any, Callable, Dict, List, Optional, Tuple

from botocore.exceptions import BotoCoreError, ClientError

from utility import athena_query
from utility import failure_catalog as catalog
from utility.connector_config import athena_table
from utility.connector_utility import SourceAccessError

logger = logging.getLogger(__name__)

STATUS_SUCCESS = "SUCCESS"
STATUS_RECON_FAILED = "RECON_FAILED"
STATUS_FAILED = "FAILED"

OUTCOME_NO_DATA = "NO_DATA_THIS_MONTH"

REQUIRED_FIELDS = ("last_modified_ts", "status")

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
        return self.outcome == OUTCOME_NO_DATA

    @property
    def blocking_scenario(self) -> catalog.Scenario:
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
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if not isinstance(value, str):
        return None
    return athena_query.parse_timestamp(value)


def _as_int(value: Any) -> Optional[int]:
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
    return str(name).replace("`", "").strip().lower()


def recon_query(table: str, target: str) -> Tuple[str, List[str]]:
    sql = (
        f"SELECT * FROM {athena_table(table)} "
        "WHERE lower(replace(target_table_name, '`', '')) = ? "
        "ORDER BY last_modified_ts DESC "
        "LIMIT 1"
    )
    return sql, [athena_query.string_literal(normalise_target(target))]


def _json_safe(row: Dict[str, Any]) -> Dict[str, Any]:
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
        return _blocked(
            "UPSTREAM_RECON_UNREADABLE",
            catalog.SCHEMA_VALIDATION_FAILURE,
            f"The newest reconciliation row for {target} has status {status!r}; the "
            f"contract is {STATUS_SUCCESS}, {STATUS_RECON_FAILED} or {STATUS_FAILED} {details}",
            **common,
        )

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
