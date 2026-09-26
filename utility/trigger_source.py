"""The trigger event source: one Athena (Iceberg) table per trigger.

Athena is the only source this project reads. ``IFC_RUN__TRIGGER`` picks the
table, the query selects the month's rows by ``business_date``, and each row is
yielded as a ``TriggerEvent`` whose attributes are the row's columns.

A row that cannot become an event, or a query that cannot be run, yields a
``ParseFailure`` rather than raising, so the runner can classify and report it
instead of the task dying on an unhandled exception.
"""

from __future__ import annotations

import calendar
import json
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple, Union

from botocore.exceptions import BotoCoreError, ClientError

from utility import failure_catalog as catalog
from utility.error_classifier import RecordRejected
from utility.connector_utility import SourceAccessError
from utility.connector_config import SourceSettings, athena_table, sql_identifier
from utility.run_gate import today
from utility.tb_outcome_schema import TRIGGER_TYPE, TriggerEvent, previous_month

logger = logging.getLogger(__name__)


@dataclass
class ParseFailure:
    """A record that could not even be parsed into a ``TriggerEvent``."""

    source_object: str
    index: int
    error: str
    raw: Optional[str] = None
    scenario_key: str = catalog.SCHEMA_VALIDATION_FAILURE.key


SourceItem = Union[TriggerEvent, ParseFailure]


# ---------------------------------------------------------------------------
# Athena
# ---------------------------------------------------------------------------

#: Athena result types that are read back as something other than a string.
#: Everything arrives from ``GetQueryResults`` as ``VarCharValue`` text; these
#: restore the column's type, so a bigint CSID is an int and a date is a date by
#: the time the payload builder sees it.
_INTEGER_TYPES = {"tinyint", "smallint", "integer", "int", "bigint"}
_FLOAT_TYPES = {"double", "float", "real"}
_NESTED_TYPES = ("array", "map", "row", "struct", "json")

_TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}


def _coerce(value: Optional[str], athena_type: str) -> Any:
    """One ``VarCharValue`` as the Python value its Athena column type implies.

    A value that does not parse as its declared type is passed through as text:
    the payload builder then accepts or rejects it per field, which reports the
    bad column by name rather than failing the whole row here.
    """
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
        if kind == "timestamp":
            return datetime.fromisoformat(value.replace(" ", "T", 1))
        if kind.startswith(_NESTED_TYPES):
            # Only JSON-typed or json_format()'d columns come back as JSON; a
            # raw struct renders as {a=1, b=2}, which stays a string.
            return json.loads(value)
    except (ValueError, ArithmeticError):
        return value
    return value


class AthenaTriggerSource:
    """Reads one trigger's latest month straight from its Iceberg table.

    The runner uses ``stream``, ``first_object`` (preflight) and
    ``objects_read`` (audit trail).

    ``IFC_RUN__TRIGGER`` picks the table (``source.trigger_tables``) and is the
    only thing that says which trigger a row belongs to: the table holds just
    the attribute columns. The envelope then publishes ``triggerType`` as the
    constant ``KYCRefresh`` and ``triggerSubType`` as the trigger's published
    symbol (TRIGGER_8 -> NewHRCRelationship, TRIGGER_9 -> AccountInactivity,
    TRIGGER_21 -> MultipleTMSARs).

    The latest month is the rows whose ``business_date`` is the last day of the
    previous month - upstream stamps every row of a month with that date - and
    the run gate stops a delivered month being read twice.

    Monthly volumes are in the hundreds, so the whole result is paged through
    ``GetQueryResults`` (1000 rows a page) with no ``UNLOAD`` step.
    """

    def __init__(
        self,
        settings: SourceSettings,
        *,
        trigger: Optional[str],
        business_month: Optional[str] = None,
        client: Any = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if not trigger:
            raise ValueError("The Athena source needs the run's trigger (IFC_RUN__TRIGGER)")
        self._settings = settings
        self._athena = settings.athena
        self._trigger = trigger
        self._business_month = business_month or previous_month(today())
        self._client = client
        self._sleep = sleep
        self._objects_read: List[str] = []
        self.last_query_execution_id: Optional[str] = None

    # -- identity ------------------------------------------------------------

    @property
    def table(self) -> str:
        return self._settings.resolved_table

    @property
    def object_id(self) -> str:
        """A stable name for this run's batch.

        Stable across re-runs of the same month, so it works as the checkpoint
        key ``skip_objects`` compares against, and it is what the run summary
        and manifest record as the source that was read.
        """
        return (
            f"athena://{self._athena.catalog}/{self.table}"
            f"?{self._athena.business_date_column}={self.business_date.isoformat()}"
        )

    @property
    def business_date(self) -> date:
        """The ``business_date`` this run reads: the last day of the business month.

        The business month is the month before the run date, so a run on any day
        in September 2026 reads ``2026-08-31``.
        """
        year, month = (int(part) for part in self._business_month.split("-"))
        return date(year, month, calendar.monthrange(year, month)[1])

    @property
    def objects_read(self) -> List[str]:
        return list(self._objects_read)

    def _athena_client(self) -> Any:
        if self._client is None:
            import boto3
            from botocore.config import Config

            # Standard retry mode backs off on TooManyRequestsException, which is
            # what a busy shared workgroup returns.
            self._client = boto3.client(
                "athena", config=Config(retries={"mode": "standard", "max_attempts": 5})
            )
        return self._client

    # -- query ---------------------------------------------------------------

    def query(self) -> Tuple[str, List[str]]:
        """The SQL and its execution parameters.

        Identifiers are validated and quoted (they cannot be parameters); the
        business date is bound as a parameter. ``CAST(... AS DATE)`` lets the
        column be a date, a timestamp at midnight or an ISO date string alike.
        """
        column = sql_identifier(self._athena.business_date_column, what="business_date_column")
        sql = f"SELECT * FROM {athena_table(self.table)} WHERE CAST({column} AS DATE) = CAST(? AS DATE)"
        if self._athena.order_by:
            order = ", ".join(sql_identifier(c, what="order_by column") for c in self._athena.order_by)
            sql += f" ORDER BY {order}"
        # Execution parameters are substituted as SQL literals, so a string
        # carries its own quotes. The value is generated here from a date.
        return sql, [f"'{self.business_date.isoformat()}'"]

    def _start(self) -> str:
        sql, parameters = self.query()
        request: Dict[str, Any] = {
            "QueryString": sql,
            "ExecutionParameters": parameters,
            "WorkGroup": self._athena.workgroup,
            "QueryExecutionContext": {"Catalog": self._athena.catalog},
        }
        if self._athena.output_location:
            request["ResultConfiguration"] = {"OutputLocation": self._athena.output_location}

        query_id = self._athena_client().start_query_execution(**request)["QueryExecutionId"]
        self.last_query_execution_id = query_id
        logger.info(
            "Athena query started",
            extra={
                "query_execution_id": query_id,
                "table": self.table,
                "trigger": self._trigger,
                "business_date": self.business_date.isoformat(),
                "workgroup": self._athena.workgroup,
            },
        )
        return query_id

    def _wait(self, query_id: str) -> None:
        client = self._athena_client()
        deadline = time.monotonic() + self._athena.query_timeout_seconds
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
                    f"{self._athena.query_timeout_seconds}s and was cancelled",
                    path=self.table,
                    operation="query",
                )
            self._sleep(self._athena.poll_interval_seconds)

        if state != "SUCCEEDED":
            reason = status.get("StateChangeReason") or "no reason given"
            raise SourceAccessError(
                f"Athena query {query_id} {state}: {reason}", path=self.table, operation="query"
            )

        statistics = execution.get("Statistics", {})
        logger.info(
            "Athena query finished",
            extra={
                "query_execution_id": query_id,
                "data_scanned_bytes": statistics.get("DataScannedInBytes"),
                "engine_ms": statistics.get("EngineExecutionTimeInMillis"),
            },
        )

    def _rows(self, query_id: str) -> Iterator[Dict[str, Any]]:
        paginator = self._athena_client().get_paginator("get_query_results")
        columns: Optional[List[Tuple[str, str]]] = None
        for page in paginator.paginate(QueryExecutionId=query_id):
            result_set = page["ResultSet"]
            rows = result_set.get("Rows", [])
            if columns is None:
                columns = [
                    (info["Name"], info.get("Type", "varchar"))
                    for info in result_set["ResultSetMetadata"]["ColumnInfo"]
                ]
                # A SELECT's first row is the header row.
                rows = rows[1:]
            for row in rows:
                cells = row.get("Data", [])
                yield {
                    name: _coerce(cells[i].get("VarCharValue") if i < len(cells) else None, kind)
                    for i, (name, kind) in enumerate(columns)
                }

    # -- runner surface --------------------------------------------------------

    def first_object(self) -> Optional[str]:
        """Preflight probe: confirm the table exists and is visible to this role.

        Cheap (a Glue catalogue lookup, no data scanned) and exercises the same
        permissions the query needs. Raises on failure, which preflight reports
        as a BDP read failure.
        """
        database, table = self.table.split(".")
        self._athena_client().get_table_metadata(
            CatalogName=self._athena.catalog, DatabaseName=database, TableName=table
        )
        return self.object_id

    def selected_objects(self) -> List[str]:
        return [self.object_id]

    def stream(self, *, skip_objects: Optional[Set[str]] = None, limit: Optional[int] = None) -> Iterator[SourceItem]:
        object_id = self.object_id
        if object_id in (skip_objects or set()):
            logger.debug("Skipping already-processed batch", extra={"source_object": object_id})
            return

        self._objects_read.append(object_id)
        max_records = limit or self._settings.max_records_per_batch

        try:
            query_id = self._start()
            self._wait(query_id)
            for index, row in enumerate(self._rows(query_id)):
                # The row's columns are the event's attributes; the trigger is
                # the run's own.
                yield from self._to_event(row, object_id, index)
                if max_records is not None and index + 1 >= max_records:
                    logger.warning(
                        "Record limit reached; the rest of this month's rows are NOT published",
                        extra={"limit": max_records, "source_object": object_id},
                    )
                    return
        except (SourceAccessError, ClientError, BotoCoreError) as exc:
            # A BDP read failure, so the runner fails the run as SOURCE_UNREADABLE
            # rather than ZERO_RECORDS.
            logger.error(
                "Could not read the Athena source",
                extra={"source_object": object_id, "error": str(exc)},
            )
            yield ParseFailure(object_id, 0, str(exc), scenario_key=catalog.BDP_READ_FAILURE.key)

    def _to_event(self, row: Dict[str, Any], object_id: str, index: int) -> Iterator[SourceItem]:
        # The table holds only the attribute columns; the wrapper around them is
        # built from the run. triggerType is informational here - the envelope
        # always publishes TRIGGER_TYPE whatever an event says.
        wrapper: Dict[str, Any] = {
            "triggerType": TRIGGER_TYPE,
            "triggerSubType": self._trigger,
            "attributes": row,
        }
        if self._athena.upstream_trigger_id_column:
            upstream_id = row.get(self._athena.upstream_trigger_id_column)
            wrapper["upstreamTriggerId"] = None if upstream_id is None else str(upstream_id)
        try:
            yield TriggerEvent.from_dict(wrapper, source_object=object_id, index=index)
        except RecordRejected as exc:
            yield ParseFailure(object_id, index, str(exc), raw=json.dumps(row, default=str)[:2000])


def make_source(settings: SourceSettings, *, trigger: Optional[str] = None) -> "AthenaTriggerSource":
    """The reader for this run's trigger table."""
    return AthenaTriggerSource(settings, trigger=trigger)
