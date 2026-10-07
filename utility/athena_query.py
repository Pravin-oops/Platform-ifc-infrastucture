"""Running one Athena query and reading its rows back."""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from utility.connector_utility import SourceAccessError

logger = logging.getLogger(__name__)

#: Athena types read back as their Python type; everything else stays text.
_INTEGER_TYPES = {"tinyint", "smallint", "integer", "int", "bigint"}
_FLOAT_TYPES = {"double", "float", "real"}
_NESTED_TYPES = ("array", "map", "row", "struct", "json")

_TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}


def parse_timestamp(text: str) -> Optional[datetime]:
    """Timestamp text as a naive UTC datetime, or ``None`` if it is not one."""
    if not isinstance(text, str) or not text.strip():
        return None
    value = text.strip()

    zone = None
    head, sep, tail = value.rpartition(" ")
    if sep and tail[:1].isalpha():
        # A trailing zone name: Athena renders timestamp with time zone as
        # '<timestamp> UTC'. fromisoformat does not read names.
        try:
            from zoneinfo import ZoneInfo

            zone = ZoneInfo(tail)
        except Exception:
            return None
        value = head

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None

    if zone is not None and parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=zone)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def coerce(value: Optional[str], athena_type: str) -> Any:
    """One ``VarCharValue`` as the Python value its Athena column type implies."""
    if value is None:
        return None
    kind = athena_type.lower().split("(", 1)[0].strip()
    try:
        if kind in _INTEGER_TYPES:
            return int(value)
        if kind in _FLOAT_TYPES:
            return float(value)
        if kind == "decimal":
            return Decimal(value)
        if kind == "boolean":
            return value.strip().lower() == "true"
        if kind == "date":
            return date.fromisoformat(value)
        if kind.startswith("timestamp"):
            # timestamp, timestamp(3), timestamp with time zone - the last
            # renders with a zone name ('... UTC') that fromisoformat cannot read.
            parsed = parse_timestamp(value)
            return value if parsed is None else parsed
        if kind.startswith(_NESTED_TYPES):
            # Only JSON-typed or json_format()'d columns come back as JSON; a
            # raw struct renders as {a=1, b=2}, which stays a string.
            return json.loads(value)
    except (ValueError, ArithmeticError):
        return value
    return value


def athena_client() -> Any:
    """A boto3 Athena client. Standard retry mode backs off on
    TooManyRequestsException, which is what a busy shared workgroup returns."""
    import boto3
    from botocore.config import Config

    return boto3.client("athena", config=Config(retries={"mode": "standard", "max_attempts": 5}))


def string_literal(value: str) -> str:
    """``value`` as an Athena string literal for ``ExecutionParameters``."""
    return "'" + str(value).replace("'", "''") + "'"


def start(client: Any, athena: Any, sql: str, parameters: List[str], *, label: str) -> str:
    """Start ``sql`` in the configured workgroup; returns the query execution id."""
    request: Dict[str, Any] = {
        "QueryString": sql,
        "ExecutionParameters": parameters,
        "WorkGroup": athena.workgroup,
        "QueryExecutionContext": {"Catalog": athena.catalog},
    }
    if athena.output_location:
        request["ResultConfiguration"] = {"OutputLocation": athena.output_location}

    query_id = client.start_query_execution(**request)["QueryExecutionId"]
    logger.debug(
        "Athena query started",
        extra={"query_execution_id": query_id, "table": label, "workgroup": athena.workgroup},
    )
    return query_id


def wait(
    client: Any,
    athena: Any,
    query_id: str,
    *,
    label: str,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Poll until the query finishes; raise ``SourceAccessError`` unless it succeeded."""
    deadline = time.monotonic() + athena.query_timeout_seconds
    while True:
        execution = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        status = execution["Status"]
        state = status["State"]
        if state in _TERMINAL_STATES:
            break
        if time.monotonic() >= deadline:
            client.stop_query_execution(QueryExecutionId=query_id)
            raise SourceAccessError(
                f"Athena query {query_id} did not finish within "
                f"{athena.query_timeout_seconds}s and was cancelled",
                path=label,
                operation="query",
            )
        sleep(athena.poll_interval_seconds)

    if state != "SUCCEEDED":
        reason = status.get("StateChangeReason") or "no reason given"
        raise SourceAccessError(f"Athena query {query_id} {state}: {reason}", path=label, operation="query")

    statistics = execution.get("Statistics", {})
    logger.info(
        "Athena query finished",
        extra={
            "query_execution_id": query_id,
            "data_scanned_bytes": statistics.get("DataScannedInBytes"),
            "engine_ms": statistics.get("EngineExecutionTimeInMillis"),
        },
    )


def rows(client: Any, query_id: str) -> Iterator[Dict[str, Any]]:
    """Every result row as ``{column: typed value}``, paged 1000 rows at a time."""
    paginator = client.get_paginator("get_query_results")
    columns: Optional[List[Tuple[str, str]]] = None
    for page in paginator.paginate(QueryExecutionId=query_id):
        result_set = page["ResultSet"]
        page_rows = result_set.get("Rows", [])
        if columns is None:
            columns = [
                (info["Name"], info.get("Type", "varchar"))
                for info in result_set["ResultSetMetadata"]["ColumnInfo"]
            ]
            # A SELECT's first row is the header row.
            page_rows = page_rows[1:]
        for row in page_rows:
            cells = row.get("Data", [])
            yield {
                name: coerce(cells[i].get("VarCharValue") if i < len(cells) else None, kind)
                for i, (name, kind) in enumerate(columns)
            }
