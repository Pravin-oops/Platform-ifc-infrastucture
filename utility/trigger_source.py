"""The trigger event source: one Athena (Iceberg) table per trigger."""

from __future__ import annotations

import calendar
import logging
import time
from datetime import date
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from botocore.exceptions import BotoCoreError, ClientError

from utility import athena_query
from utility.connector_utility import SourceAccessError
from utility.connector_config import SourceSettings, athena_table, sql_identifier
from utility.run_gate import execution_date
from utility.tb_outcome_schema import TRIGGER_TYPE, TriggerEvent, previous_month

logger = logging.getLogger(__name__)


class AthenaTriggerSource:
    """Reads one trigger's latest month straight from its Iceberg table."""

    def __init__(
        self,
        settings: SourceSettings,
        *,
        trigger: str,
        business_month: Optional[str] = None,
        client: Any = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._settings = settings
        self._athena = settings.athena
        self._trigger = trigger
        self._business_month = business_month or previous_month(execution_date())
        self._client = client
        self._sleep = sleep
        self.last_query_execution_id: Optional[str] = None

    @property
    def table(self) -> str:
        return self._settings.resolved_table

    @property
    def business_date(self) -> date:
        """The ``business_date`` this run reads: the last day of the business month."""
        year, month = (int(part) for part in self._business_month.split("-"))
        return date(year, month, calendar.monthrange(year, month)[1])

    def describe(self) -> Dict[str, Any]:
        """What this run read: the table, the business date and the query."""
        return {
            "table": self.table,
            "business_date": self.business_date.isoformat(),
            "query_execution_id": self.last_query_execution_id,
        }

    def _athena_client(self) -> Any:
        if self._client is None:
            self._client = athena_query.athena_client()
        return self._client

    def query(self) -> Tuple[str, List[str]]:
        """The SQL and its execution parameters."""
        column = sql_identifier(self._athena.business_date_column, what="business_date_column")
        sql = f"SELECT * FROM {athena_table(self.table)} WHERE CAST({column} AS DATE) = CAST(? AS DATE)"
        if self._athena.order_by:
            order = ", ".join(sql_identifier(c, what="order_by column") for c in self._athena.order_by)
            sql += f" ORDER BY {order}"
        return sql, [athena_query.string_literal(self.business_date.isoformat())]

    def check_access(self) -> str:
        """Preflight probe: confirm the table exists and is visible to this role."""
        database, table = self.table.split(".")
        self._athena_client().get_table_metadata(
            CatalogName=self._athena.catalog, DatabaseName=database, TableName=table
        )
        return self.table

    def stream(self) -> Iterator[TriggerEvent]:
        """Yield the month's rows as events. Raises ``SourceAccessError`` when the
        query cannot be run."""
        client = self._athena_client()
        try:
            sql, parameters = self.query()
            query_id = athena_query.start(client, self._athena, sql, parameters, label=self.table)
            self.last_query_execution_id = query_id
            logger.info(
                "Reading the trigger table",
                extra={
                    "query_execution_id": query_id,
                    "trigger": self._trigger,
                    "business_date": self.business_date.isoformat(),
                },
            )
            athena_query.wait(client, self._athena, query_id, label=self.table, sleep=self._sleep)
            for index, row in enumerate(athena_query.rows(client, query_id)):
                yield self._to_event(row, index)
        except (ClientError, BotoCoreError) as exc:
            raise SourceAccessError(
                f"Athena query on {self.table} failed: {exc}",
                path=self.table,
                operation="query",
                cause=exc,
            ) from exc

    def _to_event(self, row: Dict[str, Any], index: int) -> TriggerEvent:
        # The table holds only the attribute columns; the trigger is the run's own.
        return TriggerEvent(
            trigger_sub_type=self._trigger,
            attributes=row,
            trigger_type=TRIGGER_TYPE,
            source_object=self.table,
            source_index=index,
        )


def make_source(
    settings: SourceSettings, *, trigger: str, business_month: Optional[str] = None
) -> AthenaTriggerSource:
    """The reader for this run's trigger table and business month."""
    return AthenaTriggerSource(settings, trigger=trigger, business_month=business_month)

