"""The Athena source - reading a trigger's month straight from its table.

Every test drives ``AthenaTriggerSource`` through a fake Athena client that
answers the four calls the reader makes, so nothing here needs AWS.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pytest
from botocore.exceptions import ClientError

from utility import failure_catalog as catalog
from utility.connector_config import SourceSettings, load_settings
from utility.connector_utility import load_schema_document
from utility.tb_outcome_schema import EnvelopeBuilder, TriggerEvent
from utility.trigger_source import (
    AthenaTriggerSource,
    ParseFailure,
    _coerce,
    make_source,
)

TABLE = "ifc_trigger_db.trigger_8_events"

COLUMNS = [
    ("date_of_request", "timestamp"),
    ("counterparty_full_legal_entity_name", "varchar"),
    ("counterparty_csid_sds", "bigint"),
    ("customer_segment", "varchar"),
    ("client_relationship_owner_brid", "varchar"),
    ("client_relationship_owner_name", "varchar"),
    ("client_relationship_owner_business_unit", "varchar"),
    ("client_relationship_owner_location", "varchar"),
    ("region", "varchar"),
    ("business_date", "date"),
]

ROW = [
    "2026-08-10 02:15:04.221",
    "AbCdEfGh12345",
    "9912345678",
    "Corporate",
    "B0412775",
    "XyZwVu67890",
    "UK Corporate",
    "UK",
    "EMEA",
    "2026-08-31",
]


def result_page(rows: List[List[Optional[str]]], *, header: bool) -> Dict[str, Any]:
    """One GetQueryResults page. Only the first page of a SELECT has the header."""
    data = [{"Data": [{"VarCharValue": name} for name, _ in COLUMNS]}] if header else []
    data += [{"Data": [{} if v is None else {"VarCharValue": v} for v in row]} for row in rows]
    return {
        "ResultSet": {
            "Rows": data,
            "ResultSetMetadata": {"ColumnInfo": [{"Name": n, "Type": t} for n, t in COLUMNS]},
        }
    }


class FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **_kwargs):
        return iter(self._pages)


class FakeAthena:
    def __init__(self, *, pages=None, states=("RUNNING", "SUCCEEDED"), reason=None, start_error=None):
        self.pages = pages if pages is not None else [result_page([ROW], header=True)]
        self.states = list(states)
        self.reason = reason
        self.start_error = start_error
        self.started: List[Dict[str, Any]] = []
        self.stopped: List[str] = []
        self.metadata_calls: List[Dict[str, Any]] = []

    def start_query_execution(self, **request):
        if self.start_error:
            raise self.start_error
        self.started.append(request)
        return {"QueryExecutionId": "q-1"}

    def get_query_execution(self, QueryExecutionId):
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        status = {"State": state}
        if self.reason:
            status["StateChangeReason"] = self.reason
        return {"QueryExecution": {"Status": status, "Statistics": {"DataScannedInBytes": 1024}}}

    def stop_query_execution(self, QueryExecutionId):
        self.stopped.append(QueryExecutionId)

    def get_paginator(self, name):
        assert name == "get_query_results"
        return FakePaginator(self.pages)

    def get_table_metadata(self, **kwargs):
        self.metadata_calls.append(kwargs)
        return {"TableMetadata": {"Name": kwargs["TableName"]}}


def athena_settings(**athena) -> SourceSettings:
    return SourceSettings(table=TABLE, athena=athena)


def make(client, *, month="2026-08", trigger="TRIGGER_8", **athena) -> AthenaTriggerSource:
    return AthenaTriggerSource(
        athena_settings(**athena),
        trigger=trigger,
        business_month=month,
        client=client,
        sleep=lambda _s: None,
    )


class TestQuery:
    def test_it_reads_the_last_day_of_the_business_month(self):
        sql, params = make(FakeAthena()).query()

        assert sql == (
            'SELECT * FROM "ifc_trigger_db"."trigger_8_events" '
            'WHERE CAST("business_date" AS DATE) = CAST(? AS DATE)'
        )
        assert params == ["'2026-08-31'"]

    @pytest.mark.parametrize(
        "month, last_day",
        [
            ("2026-09", "2026-09-30"),
            ("2026-12", "2026-12-31"),
            ("2027-02", "2027-02-28"),
            ("2028-02", "2028-02-29"),  # leap year
        ],
    )
    def test_the_last_day_follows_the_calendar(self, month, last_day):
        _, params = make(FakeAthena(), month=month).query()
        assert params == [f"'{last_day}'"]

    def test_by_default_the_business_month_is_the_one_before_the_run_date(self, monkeypatch):
        monkeypatch.setattr("utility.trigger_source.today", lambda: date(2026, 9, 25))
        source = AthenaTriggerSource(athena_settings(), trigger="TRIGGER_8", client=FakeAthena())
        assert source.business_date == date(2026, 8, 31)

    def test_january_reads_the_previous_december(self, monkeypatch):
        monkeypatch.setattr("utility.trigger_source.today", lambda: date(2027, 1, 5))
        source = AthenaTriggerSource(athena_settings(), trigger="TRIGGER_8", client=FakeAthena())
        assert source.business_date == date(2026, 12, 31)

    def test_the_date_column_and_order_are_configurable(self):
        sql, _ = make(FakeAthena(), business_date_column="biz_dt", order_by=["date_of_request"]).query()
        assert 'CAST("biz_dt" AS DATE)' in sql
        assert sql.endswith('ORDER BY "date_of_request"')

    def test_the_request_names_the_workgroup_and_optional_output_location(self):
        client = FakeAthena()
        list(make(client, workgroup="ifc", output_location="s3://results/ifc/").stream())

        request = client.started[0]
        assert request["WorkGroup"] == "ifc"
        assert request["ResultConfiguration"] == {"OutputLocation": "s3://results/ifc/"}
        assert request["ExecutionParameters"] == ["'2026-08-31'"]

    def test_without_an_output_location_the_workgroup_decides(self):
        client = FakeAthena()
        list(make(client).stream())
        assert "ResultConfiguration" not in client.started[0]


class TestIdentifierValidation:
    @pytest.mark.parametrize(
        "table",
        ["trigger_8_events", "db.table.extra", 'db."x"; DROP TABLE y', "db.tab le", "db.1table"],
    )
    def test_a_table_that_is_not_database_dot_table_is_rejected(self, table):
        with pytest.raises(ValueError):
            SourceSettings(table=table)

    def test_per_trigger_tables_are_validated_too(self):
        with pytest.raises(ValueError):
            SourceSettings(trigger_tables={"TRIGGER_8": "not-a-table"})

    def test_a_bad_column_name_is_rejected(self):
        with pytest.raises(ValueError):
            athena_settings(business_date_column="business_date; --")

    @pytest.mark.parametrize(
        "legacy",
        [
            {"type": "s3", "table": TABLE},
            {"path": "s3://bucket/trigger_8/{MONTH}_{YYYY}/"},
            {"trigger_paths": {"TRIGGER_8": "s3://bucket/trigger_8/"}},
            {"table": TABLE, "file_suffixes": [".json"]},
            {"table": TABLE, "archive_path": "s3://bucket/archive/"},
        ],
    )
    def test_the_old_s3_extract_settings_are_refused(self, legacy):
        """Athena is the only permitted source; a leftover S3 setting must fail
        at load rather than be silently ignored."""
        with pytest.raises(ValueError, match="Extra inputs are not permitted|trigger_tables"):
            SourceSettings(**legacy)


class TestStream:
    def test_each_row_becomes_an_event_for_the_run_trigger(self):
        source = make(FakeAthena())
        items = list(source.stream())

        assert len(items) == 1
        event = items[0]
        assert isinstance(event, TriggerEvent)
        assert event.trigger_sub_type == "TRIGGER_8"
        assert event.csid == "9912345678"
        assert event.source_object == "athena://AwsDataCatalog/ifc_trigger_db.trigger_8_events?business_date=2026-08-31"
        assert source.objects_read == [event.source_object]
        assert source.last_query_execution_id == "q-1"

    def test_column_types_are_restored(self):
        attributes = list(make(FakeAthena()).stream())[0].attributes

        assert attributes["counterparty_csid_sds"] == 9912345678
        assert attributes["business_date"] == date(2026, 8, 31)
        assert attributes["date_of_request"] == datetime(2026, 8, 10, 2, 15, 4, 221000)
        assert attributes["region"] == "EMEA"

    def test_a_null_cell_is_none(self):
        row = list(ROW)
        row[3] = None  # customer_segment
        client = FakeAthena(pages=[result_page([row], header=True)])
        assert list(make(client).stream())[0].attributes["customer_segment"] is None

    def test_rows_are_read_across_pages_with_one_header(self):
        client = FakeAthena(
            pages=[result_page([ROW, ROW], header=True), result_page([ROW], header=False)]
        )
        items = list(make(client).stream())
        assert len(items) == 3
        assert [i.source_index for i in items] == [0, 1, 2]

    def test_an_empty_month_yields_nothing(self):
        client = FakeAthena(pages=[result_page([], header=True)])
        assert list(make(client).stream()) == []

    def test_an_already_processed_month_is_skipped_without_querying(self):
        client = FakeAthena()
        source = make(client)
        assert list(source.stream(skip_objects={source.object_id})) == []
        assert client.started == []

    def test_the_limit_caps_the_rows(self):
        client = FakeAthena(pages=[result_page([ROW, ROW, ROW], header=True)])
        assert len(list(make(client).stream(limit=2))) == 2

    def test_the_upstream_trigger_id_can_come_from_a_column(self):
        event = list(make(FakeAthena(), upstream_trigger_id_column="counterparty_csid_sds").stream())[0]
        assert event.upstream_trigger_id == "9912345678"

    def test_without_that_column_the_upstream_id_is_none(self):
        assert list(make(FakeAthena()).stream())[0].upstream_trigger_id is None


class TestFailures:
    def _only_failure(self, source) -> ParseFailure:
        items = list(source.stream())
        assert len(items) == 1 and isinstance(items[0], ParseFailure)
        assert items[0].scenario_key == catalog.BDP_READ_FAILURE.key
        return items[0]

    def test_a_failed_query_is_a_source_read_failure(self):
        client = FakeAthena(states=["FAILED"], reason="TABLE_NOT_FOUND: line 1:15")
        failure = self._only_failure(make(client))
        assert "TABLE_NOT_FOUND" in failure.error

    def test_a_cancelled_query_is_a_source_read_failure(self):
        self._only_failure(make(FakeAthena(states=["CANCELLED"])))

    def test_an_api_error_is_a_source_read_failure(self):
        error = ClientError({"Error": {"Code": "AccessDeniedException", "Message": "no"}}, "StartQueryExecution")
        failure = self._only_failure(make(FakeAthena(start_error=error)))
        assert "AccessDeniedException" in failure.error

    def test_a_query_past_its_timeout_is_cancelled(self, monkeypatch):
        clock = iter([0.0, 0.0, 400.0])
        monkeypatch.setattr("utility.trigger_source.time.monotonic", lambda: next(clock))
        client = FakeAthena(states=["RUNNING"])

        failure = self._only_failure(make(client, query_timeout_seconds=300))

        assert client.stopped == ["q-1"]
        assert "cancelled" in failure.error


class TestPreflightProbe:
    def test_it_checks_the_table_without_running_a_query(self):
        client = FakeAthena()
        source = make(client)

        assert source.first_object() == source.object_id
        assert client.metadata_calls == [
            {"CatalogName": "AwsDataCatalog", "DatabaseName": "ifc_trigger_db", "TableName": "trigger_8_events"}
        ]
        assert client.started == []


class TestFactory:
    def test_athena_type_builds_the_athena_reader(self):
        source = make_source(athena_settings(), trigger="TRIGGER_8")
        assert isinstance(source, AthenaTriggerSource)

    def test_athena_needs_the_run_trigger(self):
        with pytest.raises(ValueError, match="IFC_RUN__TRIGGER"):
            make_source(athena_settings(), trigger=None)


class TestCoerce:
    @pytest.mark.parametrize(
        "value, kind, expected",
        [
            ("42", "integer", 42),
            ("1.5", "double", 1.5),
            ("10.25", "decimal(18,2)", Decimal("10.25")),
            ("true", "boolean", True),
            ("false", "boolean", False),
            ('{"a": 1}', "json", {"a": 1}),
            ("{a=1, b=2}", "row(a integer, b integer)", "{a=1, b=2}"),
            ("not-a-number", "bigint", "not-a-number"),
            ("2026-08-10 02:15:04.221 UTC", "timestamp with time zone", "2026-08-10 02:15:04.221 UTC"),
        ],
    )
    def test_values_take_their_column_type(self, value, kind, expected):
        assert _coerce(value, kind) == expected


class TestEndToEnd:
    def test_an_athena_row_builds_a_schema_valid_envelope(self):
        builder = EnvelopeBuilder(
            avro_schema=load_schema_document("utility/schema.json"), business_month="2026-08"
        )
        event = list(make(FakeAthena()).stream())[0]

        built = builder.build(event)

        assert built.record["triggerSubType"] == "NewHRCRelationship"
        assert built.record["idValue"] == "9912345678"

    @pytest.mark.parametrize(
        "run_trigger, table, published",
        [
            ("TRIGGER_8", '"bdp_corp_ifc_trigger_8"', "NewHRCRelationship"),
            ("TRIGGER_9", '"bdp_corp_ifc_trigger_9"', "AccountInactivity"),
            ("TRIGGER_21", '"bdp_corp_ifc_trigger_21"', "MultipleTMSARs"),
        ],
    )
    def test_ifc_run_trigger_picks_the_table_and_the_published_sub_type(
        self, clean_ifc_env, monkeypatch, run_trigger, table, published
    ):
        """The path main_ecs.py takes: IFC_RUN__TRIGGER -> table -> envelope."""
        monkeypatch.setenv("IFC_RUN__TRIGGER", run_trigger)
        settings = load_settings("utility/connector_config.yaml")
        settings.select_trigger(None)

        client = FakeAthena()
        source = AthenaTriggerSource(
            settings.source, trigger=settings.run.trigger, business_month="2026-08", client=client
        )
        event = list(source.stream())[0]
        built = EnvelopeBuilder(
            avro_schema=load_schema_document("utility/schema.json"), business_month="2026-08"
        ).build(event)

        assert table in client.started[0]["QueryString"]
        assert client.started[0]["ExecutionParameters"] == ["'2026-08-31'"]
        assert built.record["triggerType"] == "KYCRefresh"
        assert built.record["triggerSubType"] == published
