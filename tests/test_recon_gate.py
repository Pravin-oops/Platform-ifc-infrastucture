"""The upstream reconciliation gate.

The Databricks recon job appends a row per model run to a recon table in
Athena. The connector reads the newest row for its trigger's Databricks table
and refuses to publish unless it is from the current month, SUCCESS, and has
matching non-zero counts. Every refusal carries a catalogue exit code, because
on a monthly schedule the stopped-task record is what gets read, not the logs.

Stopping is the safe direction throughout: anything unreadable or unrecognised
blocks rather than letting the run proceed on an assumption.

Every test drives the gate through a fake Athena client that serves recon rows
the way ``GetQueryResults`` does, applying the query's own filter, order and
limit, so nothing here needs AWS.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List

import pytest
from botocore.exceptions import ClientError

from utility import athena_query
from utility import failure_catalog as catalog
from utility import recon_gate
from utility.connector_config import AthenaSettings, ReconSettings
from utility.run_gate import GateOutcome

MONTH = "2026-09"
TABLE = "ifc_recon_db.recon_audit"
TARGET_8 = "`sit_cds_snsvc0080860_prepared_db`.bdb_ifc_synthetic_data_test.BDP_Corp_IFC_Trigger_8"
TARGET_9 = "`sit_cds_snsvc0080860_prepared_db`.bdb_ifc_synthetic_data_test.BDP_Corp_IFC_Trigger_9"

#: The recon table's columns as Athena reports them.
RECON_COLUMNS = [
    ("idempotency_key", "varchar"),
    ("batch_id", "bigint"),
    ("model_name", "varchar"),
    ("target_table_name", "varchar"),
    ("job_run_id", "varchar"),
    ("env", "varchar"),
    ("dataproduct_name", "varchar"),
    ("source_count", "bigint"),
    ("target_count", "bigint"),
    ("error_record_count", "bigint"),
    ("status", "varchar"),
    ("last_modified_ts", "timestamp"),
    ("last_modified_by", "varchar"),
]


def a_row(**overrides) -> Dict[str, Any]:
    """One recon row, as Athena renders it: timestamps as ``YYYY-MM-DD HH:MM:SS.fff``."""
    row = {
        "idempotency_key": "k-0001",
        "batch_id": 7,
        "model_name": "bdp_corp_ifc_trigger_8",
        "target_table_name": TARGET_8,
        "job_run_id": "run-123",
        "env": "sit",
        "dataproduct_name": "ifc",
        "source_count": 412,
        "target_count": 412,
        "error_record_count": 0,
        "status": "SUCCESS",
        "last_modified_ts": "2026-09-30 14:30:22.123",
        "last_modified_by": "dbt",
    }
    row.update(overrides)
    return row


class FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **_kwargs):
        return iter(self._pages)


class FakeReconAthena:
    """Answers the calls the recon gate makes, serving ``rows``.

    The result applies the query's semantics - match on the normalised
    ``target_table_name``, newest ``last_modified_ts`` first, one row - so a
    test only has to put rows in the table.
    """

    def __init__(self, rows=None, *, state="SUCCEEDED", reason=None, start_error=None):
        self.rows: List[Dict[str, Any]] = rows if rows is not None else []
        self.state = state
        self.reason = reason
        self.start_error = start_error
        self.started: List[Dict[str, Any]] = []

    def start_query_execution(self, **request):
        if self.start_error:
            raise self.start_error
        self.started.append(request)
        return {"QueryExecutionId": "recon-q-1"}

    def get_query_execution(self, QueryExecutionId):
        status = {"State": self.state}
        if self.reason:
            status["StateChangeReason"] = self.reason
        return {"QueryExecution": {"Status": status}}

    def stop_query_execution(self, QueryExecutionId):
        pass

    def get_paginator(self, name):
        assert name == "get_query_results"
        wanted = self.started[-1]["ExecutionParameters"][0][1:-1].replace("''", "'")
        matching = [
            r for r in self.rows
            if recon_gate.normalise_target(r.get("target_table_name") or "") == wanted
        ]
        matching.sort(key=lambda r: str(r.get("last_modified_ts") or ""), reverse=True)

        def cell(value):
            return {} if value is None else {"VarCharValue": str(value)}

        header = {"Data": [{"VarCharValue": n} for n, _ in RECON_COLUMNS]}
        data = [{"Data": [cell(r.get(n)) for n, _ in RECON_COLUMNS]} for r in matching[:1]]
        page = {
            "ResultSet": {
                "Rows": [header] + data,
                "ResultSetMetadata": {"ColumnInfo": [{"Name": n, "Type": t} for n, t in RECON_COLUMNS]},
            }
        }
        return FakePaginator([page])


@pytest.fixture
def recon():
    client = FakeReconAthena()

    def write(row):
        client.rows.append(row)

    def replace(row):
        client.rows[:] = [row]

    def evaluate(target=TARGET_8, **athena):
        settings = ReconSettings(table=TABLE, target_table=target)
        return recon_gate.evaluate(
            settings, AthenaSettings(**athena), execution_month=MONTH,
            client=client, sleep=lambda _s: None,
        )

    return type("Recon", (), {"write": staticmethod(write), "replace": staticmethod(replace),
                              "evaluate": staticmethod(evaluate), "client": client})


class TestTheQuery:
    def test_it_asks_for_the_newest_row_naming_the_trigger(self, recon):
        recon.write(a_row())
        recon.evaluate(workgroup="ifc_wg", output_location="s3://results/athena_output/")

        (request,) = recon.client.started
        assert request["QueryString"] == (
            'SELECT * FROM "ifc_recon_db"."recon_audit" '
            "WHERE lower(replace(target_table_name, '`', '')) = ? "
            "ORDER BY last_modified_ts DESC LIMIT 1"
        )
        assert request["ExecutionParameters"] == [
            "'sit_cds_snsvc0080860_prepared_db.bdb_ifc_synthetic_data_test.bdp_corp_ifc_trigger_8'"
        ]
        assert request["WorkGroup"] == "ifc_wg"
        assert request["ResultConfiguration"] == {"OutputLocation": "s3://results/athena_output/"}

    def test_the_decision_names_the_table_target_and_query(self, recon):
        recon.write(a_row())
        reported = recon.evaluate().to_dict()
        assert reported["recon_table"] == TABLE
        assert reported["recon_target"] == TARGET_8
        assert reported["query_execution_id"] == "recon-q-1"

    def test_another_triggers_rows_are_not_read(self, recon):
        recon.write(a_row(target_table_name=TARGET_9))
        assert recon.evaluate().outcome == "UPSTREAM_DATA_NOT_RECEIVED"

    @pytest.mark.parametrize(
        "written",
        [
            "sit_cds_snsvc0080860_prepared_db.bdb_ifc_synthetic_data_test.BDP_Corp_IFC_Trigger_8",
            "`SIT_CDS_SNSVC0080860_PREPARED_DB`.BDB_IFC_SYNTHETIC_DATA_TEST.bdp_corp_ifc_trigger_8",
        ],
    )
    def test_the_target_matches_without_backticks_or_case(self, recon, written):
        recon.write(a_row(target_table_name=written))
        assert recon.evaluate().proceed

    def test_a_quote_in_the_target_is_escaped(self):
        _, params = recon_gate.recon_query(TABLE, "a'b")
        assert params == ["'a''b'"]


class TestTheNewestRowDecides:
    """A rerun appends a new row with the current timestamp."""

    def test_a_rerun_that_passed_overrides_an_earlier_failure(self, recon):
        recon.write(a_row(status="FAILED", last_modified_ts="2026-09-03 01:00:00.000"))
        recon.write(a_row(status="RECON_FAILED", last_modified_ts="2026-09-03 02:00:00.000"))
        recon.write(a_row(last_modified_ts="2026-09-03 05:00:00.000"))
        assert recon.evaluate().proceed

    def test_the_latest_last_modified_ts_wins_not_the_last_row_written(self, recon):
        recon.write(a_row(last_modified_ts="2026-09-03 05:00:00.000"))
        recon.write(a_row(status="FAILED", last_modified_ts="2026-09-03 01:00:00.000"))
        recon.write(a_row(status="RECON_FAILED", last_modified_ts="2026-09-02 23:00:00.000"))
        assert recon.evaluate().proceed

    def test_a_rerun_that_failed_overrides_an_earlier_success(self, recon):
        recon.write(a_row(last_modified_ts="2026-09-03 01:00:00.000"))
        recon.write(a_row(status="FAILED", last_modified_ts="2026-09-03 05:00:00.000"))
        assert recon.evaluate().outcome == "UPSTREAM_MODEL_FAILED"


class TestTheBlockingConditions:
    def test_no_row_is_upstream_data_not_received(self, recon):
        decision = recon.evaluate()

        assert not decision.proceed
        assert decision.outcome == "UPSTREAM_DATA_NOT_RECEIVED"
        assert decision.exit_code == catalog.TED_MISSING_SOURCE_DATA.exit_code
        assert "upstream data not received" in decision.reason.lower()

    def test_last_months_row_is_upstream_processing_not_done(self, recon):
        recon.write(a_row(last_modified_ts="2026-08-31 02:00:00.000"))
        decision = recon.evaluate()

        assert not decision.proceed
        assert decision.outcome == "UPSTREAM_PROCESSING_NOT_DONE"
        assert decision.exit_code == catalog.TED_MISSING_SOURCE_DATA.exit_code
        assert "2026-08" in decision.reason

    def test_failed_is_the_dbt_model_not_running(self, recon):
        recon.write(a_row(status="FAILED", source_count=0, target_count=0))
        decision = recon.evaluate()

        assert not decision.proceed
        assert decision.outcome == "UPSTREAM_MODEL_FAILED"
        assert decision.exit_code == catalog.TED_JOB_FAILURE.exit_code
        assert "did not run" in decision.reason
        # The alert says which model and job run, so the reader can go straight to it.
        assert "model_name=bdp_corp_ifc_trigger_8" in decision.reason
        assert "job_run_id=run-123" in decision.reason
        assert "batch_id=7" in decision.reason

    def test_recon_failed_is_an_upstream_job_failure(self, recon):
        recon.write(a_row(status="RECON_FAILED", error_record_count=17))
        decision = recon.evaluate()

        assert not decision.proceed
        assert decision.outcome == "UPSTREAM_JOB_FAILED"
        assert decision.exit_code == catalog.TED_JOB_FAILURE.exit_code
        assert "upstream job failed" in decision.reason.lower()
        assert "error_record_count=17" in decision.reason
        assert "job_run_id=run-123" in decision.reason

    def test_success_with_mismatched_counts_is_an_upstream_issue(self, recon):
        """Counts that disagree are upstream's own definition of a failed
        reconciliation, so a SUCCESS carrying them is a contradiction: upstream
        should have written RECON_FAILED. Stopping here is the backstop for
        upstream having missed it."""
        recon.write(a_row(source_count=412, target_count=400))
        decision = recon.evaluate()

        assert not decision.proceed
        assert decision.outcome == "UPSTREAM_COUNT_MISMATCH"
        assert decision.exit_code == catalog.TED_JOB_FAILURE.exit_code
        assert "412" in decision.reason and "400" in decision.reason
        assert "RECON_FAILED" in decision.reason

    @pytest.mark.parametrize(
        "source_count, target_count", [(0, 5), (5, 0), (412, 411), (1, 1000)]
    )
    def test_any_disagreement_stops_the_run(self, recon, source_count, target_count):
        recon.write(a_row(source_count=source_count, target_count=target_count))
        decision = recon.evaluate()
        assert not decision.proceed
        assert decision.outcome == "UPSTREAM_COUNT_MISMATCH"

    def test_success_with_zero_counts_is_nothing_to_process(self, recon):
        """A genuine empty month stops the run, but it is not a failure."""
        recon.write(a_row(source_count=0, target_count=0))
        decision = recon.evaluate()

        assert not decision.proceed
        assert decision.no_data
        assert decision.outcome == "NO_DATA_THIS_MONTH"
        assert decision.exit_code == catalog.EXIT_OK
        assert decision.scenario_key is None
        assert "no data to process" in decision.reason.lower()

    def test_the_blocking_outcomes_are_distinguishable(self, recon):
        """Outcomes share exit codes, so the outcome name is what tells RTB
        which one fired. They must not collide."""
        outcomes = set()
        for row in (
            a_row(last_modified_ts="2026-08-31 02:00:00.000"),
            a_row(status="FAILED"),
            a_row(status="RECON_FAILED"),
            a_row(source_count=412, target_count=400),
            a_row(source_count=0, target_count=0),
        ):
            recon.replace(row)
            outcomes.add(recon.evaluate().outcome)
        assert len(outcomes) == 5

    def test_the_upstream_failures_share_an_exit_code(self, recon):
        """FAILED, RECON_FAILED and a count mismatch are all 'upstream job
        failure', so all carry TED_JOB_FAILURE."""
        codes = set()
        for row in (
            a_row(status="FAILED"),
            a_row(status="RECON_FAILED"),
            a_row(source_count=412, target_count=400),
        ):
            recon.replace(row)
            codes.add(recon.evaluate().exit_code)
        assert codes == {catalog.TED_JOB_FAILURE.exit_code}


class TestAnUnreadableTable:
    def test_a_failed_query_blocks_as_a_read_failure(self):
        client = FakeReconAthena(state="FAILED", reason="TABLE_NOT_FOUND: recon_audit")
        decision = recon_gate.evaluate(
            ReconSettings(table=TABLE, target_table=TARGET_8), AthenaSettings(),
            execution_month=MONTH, client=client, sleep=lambda _s: None,
        )
        assert not decision.proceed
        assert decision.outcome == "UPSTREAM_RECON_UNREADABLE"
        assert decision.exit_code == catalog.BDP_READ_FAILURE.exit_code
        assert "TABLE_NOT_FOUND" in decision.reason
        assert TABLE in decision.reason

    def test_access_denied_blocks_as_a_read_failure(self):
        denied = ClientError({"Error": {"Code": "AccessDeniedException"}}, "StartQueryExecution")
        decision = recon_gate.evaluate(
            ReconSettings(table=TABLE, target_table=TARGET_8), AthenaSettings(),
            execution_month=MONTH, client=FakeReconAthena(start_error=denied),
        )
        assert decision.outcome == "UPSTREAM_RECON_UNREADABLE"
        assert decision.exit_code == catalog.BDP_READ_FAILURE.exit_code

    def test_an_unresolved_target_is_a_programming_error(self):
        with pytest.raises(ValueError):
            recon_gate.evaluate(
                ReconSettings(table=TABLE), AthenaSettings(),
                execution_month=MONTH, client=FakeReconAthena(),
            )


class TestProceeding:
    def test_this_month_success_with_data_proceeds(self, recon):
        recon.write(a_row())
        decision = recon.evaluate()

        assert decision.proceed
        assert decision.exit_code == catalog.EXIT_OK
        assert decision.document["source_count"] == 412
        # JSON-safe for the summary, manifest and alert.
        assert decision.document["last_modified_ts"] == "2026-09-30T14:30:22.123000"

    def test_a_partial_month_timestamp_still_counts_as_this_month(self, recon):
        """The gate keys on the month, not on the month having ended."""
        recon.write(a_row(last_modified_ts="2026-09-01 00:00:00.000"))
        assert recon.evaluate().proceed

    def test_error_records_proceed_but_warn(self, recon, caplog):
        recon.write(a_row(error_record_count=3))
        with caplog.at_level("WARNING"):
            assert recon.evaluate().proceed
        assert "error records" in caplog.text


class TestMalformedRows:
    """Anything unreadable blocks; none of these may fall through to publishing."""

    @pytest.mark.parametrize("field", ["last_modified_ts", "status"])
    def test_a_missing_required_field_blocks(self, recon, field):
        recon.write(a_row(**{field: None}))
        decision = recon.evaluate()
        assert decision.outcome == "UPSTREAM_RECON_UNREADABLE"
        assert field in decision.reason

    def test_an_unreadable_timestamp_blocks(self, recon):
        recon.write(a_row(last_modified_ts="not a date"))
        assert recon.evaluate().outcome == "UPSTREAM_RECON_UNREADABLE"

    def test_non_numeric_counts_block(self, recon):
        recon.write(a_row(source_count="many", target_count="more"))
        assert recon.evaluate().outcome == "UPSTREAM_RECON_UNREADABLE"

    def test_missing_counts_block(self, recon):
        recon.write(a_row(source_count=None, target_count=None))
        assert recon.evaluate().outcome == "UPSTREAM_RECON_UNREADABLE"

    @pytest.mark.parametrize("status", ["IN_PROGRESS", "OK", "SUCCEEDED", "PASSED"])
    def test_a_status_outside_the_contract_is_untrusted(self, recon, status):
        """The contract is exactly SUCCESS, RECON_FAILED or FAILED. Anything
        else means the row does not match what it was read under, so it is
        untrusted rather than another outcome to interpret - and never a green
        light."""
        recon.write(a_row(status=status))
        decision = recon.evaluate()
        assert not decision.proceed
        assert decision.outcome == "UPSTREAM_RECON_UNREADABLE"

    def test_a_lowercase_status_is_still_recognised(self, recon):
        """Case and surrounding space are normalised; the value is not."""
        recon.write(a_row(status=" success "))
        assert recon.evaluate().proceed


class TestTimestampParsing:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (datetime(2026, 9, 30, 14, 30, 22), datetime(2026, 9, 30, 14, 30, 22)),
            ("2026-09-30 14:30:22.123", datetime(2026, 9, 30, 14, 30, 22, 123000)),
            ("2026-09-30 14:30:22", datetime(2026, 9, 30, 14, 30, 22)),
            ("2026-09-30T14:30:22.123", datetime(2026, 9, 30, 14, 30, 22, 123000)),
            ("2026-09-30T14:30:22.123Z", datetime(2026, 9, 30, 14, 30, 22, 123000)),
            ("2026-09-30T14:30:22.123+00:00", datetime(2026, 9, 30, 14, 30, 22, 123000)),
        ],
    )
    def test_accepted_forms(self, value, expected):
        assert recon_gate.parse_last_modified(value) == expected

    @pytest.mark.parametrize("value", [None, "", "  ", 20260930, "30/09/2026", []])
    def test_rejected_forms(self, value):
        assert recon_gate.parse_last_modified(value) is None


class TestCountCoercion:
    @pytest.mark.parametrize(
        "raw, expected", [(0, 0), (412, 412), ("412", 412), (" 7 ", 7), (412.0, 412)]
    )
    def test_numbers_and_numeric_strings(self, raw, expected):
        assert recon_gate._as_int(raw) == expected

    @pytest.mark.parametrize("raw", ["", "many", None, 1.5, True, [], {}])
    def test_anything_else_is_none(self, raw):
        assert recon_gate._as_int(raw) is None


# ---------------------------------------------------------------------------
# Through the ECS entry point
# ---------------------------------------------------------------------------


def a_config(tmp_path, *, enabled=True, marker=True, batch_topic=None):
    config = tmp_path / "c.yaml"
    lines = [
        "app: {name: t, environment: TEST}",
        "source: {table: ifc_trigger_db.trigger_8}",
        "kafka: {topic: t, overrides: {bootstrap.servers: 'localhost:9092'}}",
        "schema_registry: {mode: DEV}",
        "health: {enabled: false}",
        f"recon: {{enabled: {str(enabled).lower()}, table: {TABLE}, "
        f"trigger_targets: {{TRIGGER_8: '{TARGET_8}'}}}}",
    ]
    if marker:
        lines.append("run_marker: {path: 's3://bucket/run-markers/run_markers.json'}")
    if batch_topic:
        lines.append(f"notifications: {{batch_sns_topic_arn: '{batch_topic}'}}")
    config.write_text("\n".join(lines), encoding="utf-8")
    return config


@pytest.fixture
def ecs(main_ecs_script, monkeypatch, tmp_path, clean_ifc_env):
    """The entry point with the run gate open, the runner fused and the recon
    table served by a fake Athena client."""
    import utility.connector_runner as runner_module
    import utility.run_gate as run_gate

    monkeypatch.setattr(run_gate, "today", lambda tz=None: date(2026, 9, 18))
    monkeypatch.setattr(
        main_ecs_script, "should_run", lambda *a, **k: GateOutcome(True, "due", False)
    )

    built = []

    class Boom:
        def __init__(self, *a, **k):
            built.append(True)
            raise AssertionError("the runner must not be built when upstream is not ready")

    monkeypatch.setattr(runner_module, "ConnectorRunner", Boom)

    alerts = []
    monkeypatch.setattr(
        main_ecs_script, "_notify", lambda *a, **k: alerts.append(a[1] if len(a) > 1 else None)
    )

    client = FakeReconAthena()
    monkeypatch.setattr(athena_query, "athena_client", lambda: client)

    def write(row):
        client.rows.append(row)

    def run(**config_kwargs):
        config = a_config(tmp_path, **config_kwargs)
        return main_ecs_script.ecs_handler(
            {"config_path": str(config), "trigger": "TRIGGER_8"}
        )

    return type("Ecs", (), {"write": staticmethod(write), "run": staticmethod(run),
                            "alerts": alerts, "built": built, "client": client})


@pytest.fixture
def batch_sent(main_ecs_script, monkeypatch):
    """Captures the batch-completion events the entry point would publish."""
    from utility.trigger_batch_notifier import TriggerBatchNotifier

    captured = []

    class Capturing(TriggerBatchNotifier):
        def publish(self, notification):
            captured.append(notification)
            return {}

    monkeypatch.setattr(main_ecs_script, "TriggerBatchNotifier", Capturing)
    return captured


class TestThroughTheEntryPoint:
    def test_a_blocked_month_never_builds_the_runner(self, ecs):
        """The whole point of gating here: no Kafka, no source, no BSP."""
        ecs.write(a_row(status="RECON_FAILED"))
        summary = ecs.run()

        assert ecs.built == []
        assert summary["outcome"] == "UPSTREAM_JOB_FAILED"
        assert summary["exit_code"] == catalog.TED_JOB_FAILURE.exit_code

    def test_a_block_raises_an_alert(self, ecs):
        """Nobody watches the logs on a monthly schedule."""
        ecs.write(a_row(source_count=412, target_count=400))
        ecs.run()
        assert len(ecs.alerts) == 1

    def test_the_summary_carries_the_recon_row(self, ecs):
        """The stopped-task record is all that survives the container."""
        ecs.write(a_row(status="RECON_FAILED", error_record_count=17))
        summary = ecs.run()

        assert summary["execution_month"] == "2026-09"
        assert summary["scenario"] == catalog.TED_JOB_FAILURE.key
        assert summary["recon"]["recon"]["error_record_count"] == 17
        assert summary["recon"]["recon"]["job_run_id"] == "run-123"
        assert summary["recon"]["recon_target"] == TARGET_8
        assert summary["recon"]["query_execution_id"] == "recon-q-1"

    def test_a_model_that_did_not_run_blocks_and_alerts(self, ecs):
        ecs.write(a_row(status="FAILED"))
        summary = ecs.run()

        assert ecs.built == []
        assert summary["outcome"] == "UPSTREAM_MODEL_FAILED"
        assert summary["exit_code"] == catalog.TED_JOB_FAILURE.exit_code
        assert len(ecs.alerts) == 1
        assert "did not run" in summary["reason"]

    def test_the_recon_query_uses_the_source_athena_settings(self, ecs):
        ecs.write(a_row())
        ecs.run()
        (request,) = ecs.client.started
        assert request["WorkGroup"] == "primary"
        assert '"ifc_recon_db"."recon_audit"' in request["QueryString"]

    def test_a_zero_count_month_never_looks_at_the_source(self, ecs):
        """A genuine empty month: the container stops at the recon document and
        never spins up Kafka, preflight or the source read."""
        ecs.write(a_row(source_count=0, target_count=0))
        summary = ecs.run()

        assert ecs.built == []
        assert summary["outcome"] == "NO_DATA_THIS_MONTH"

    def test_a_zero_count_month_is_a_success_without_an_alert(self, ecs):
        ecs.write(a_row(source_count=0, target_count=0))
        summary = ecs.run()

        assert summary["exit_code"] == catalog.EXIT_OK
        assert summary["run_status"] == "SUCCESS"
        assert summary["month_delivered"] == "2026-09"
        assert ecs.alerts == []

    def test_a_zero_count_month_sends_the_batch_event_with_zero_messages(
        self, ecs, batch_sent
    ):
        """Same body as a publishing run: zero messages, and the current time
        as both ends of the batch window."""
        from utility.tb_outcome_schema import now_timestamp

        ecs.write(a_row(source_count=0, target_count=0))
        before = now_timestamp()
        summary = ecs.run(batch_topic="arn:aws:sns:eu-west-1:1:tbb")
        after = now_timestamp()

        (event,) = batch_sent
        body = event.to_dict()
        assert list(body) == [
            "Trigger_Originating_BU", "No_Of_Messages_Produced", "Trigger_Sub_Type",
            "Topic_Name", "Trigger_Batch_Start_Timestamp", "Trigger_Batch_End_Timestamp",
            "Event_Timestamp", "Correlation_Id",
        ]
        assert body["No_Of_Messages_Produced"] == 0
        assert body["Trigger_Sub_Type"] == "NewHRCRelationship"
        assert body["Topic_Name"] == "t"
        assert body["Trigger_Originating_BU"] == "UK-C"
        assert body["Trigger_Batch_Start_Timestamp"] == body["Trigger_Batch_End_Timestamp"]
        assert before <= body["Trigger_Batch_Start_Timestamp"] <= after
        # Joins the RECON_GATE manifest this invocation wrote.
        assert body["Correlation_Id"] == summary["run_id"]

    def test_a_zero_count_month_respects_the_batch_switch(self, ecs, batch_sent):
        ecs.write(a_row(source_count=0, target_count=0))
        ecs.run()  # no batch_sns_topic_arn configured
        assert batch_sent == []

    def test_other_blocks_send_no_batch_event(self, ecs, batch_sent):
        ecs.write(a_row(status="RECON_FAILED"))
        ecs.run(batch_topic="arn:aws:sns:eu-west-1:1:tbb")
        assert batch_sent == []

    def test_a_broken_batch_topic_does_not_fail_the_empty_month(
        self, ecs, main_ecs_script, monkeypatch
    ):
        class Broken:
            def __init__(self, *a, **k):
                pass

            def publish(self, notification):
                raise RuntimeError("SNS is down")

        monkeypatch.setattr(main_ecs_script, "TriggerBatchNotifier", Broken)
        ecs.write(a_row(source_count=0, target_count=0))
        summary = ecs.run(batch_topic="arn:aws:sns:eu-west-1:1:tbb")
        assert summary["exit_code"] == catalog.EXIT_OK

    def test_a_count_mismatch_never_looks_at_the_source(self, ecs):
        ecs.write(a_row(source_count=412, target_count=400))
        summary = ecs.run()

        assert ecs.built == []
        assert summary["outcome"] == "UPSTREAM_COUNT_MISMATCH"
        assert summary["exit_code"] == catalog.TED_JOB_FAILURE.exit_code

    def test_missing_upstream_data_is_reported_not_skipped(self, ecs):
        """Distinct from the run gate's SKIPPED, which exits 0."""
        summary = ecs.run()
        assert summary["outcome"] == "UPSTREAM_DATA_NOT_RECEIVED"
        assert summary["exit_code"] == catalog.TED_MISSING_SOURCE_DATA.exit_code
        assert summary["exit_code"] != catalog.EXIT_OK

    def test_disabling_it_lets_the_run_proceed(self, ecs):
        """enabled: false reaches the runner, which is fused - the attempt to
        build it is what proves the gate did not block."""
        ecs.run(enabled=False)
        assert ecs.built, "the gate blocked a run it was configured to skip"

    def test_a_healthy_month_lets_the_run_proceed(self, ecs):
        ecs.write(a_row())
        ecs.run()
        assert ecs.built, "the gate blocked a healthy month"


class TestAReprocessChecksTheCurrentMonthsRecon:
    """Upstream writes its recon row when it runs, stamped with the current
    month, even when IFC_RUN__MONTH reprocesses an earlier one. Today is
    2026-09-18 in the ``ecs`` fixture; the reprocess is of August."""

    def test_a_current_month_row_lets_the_reprocess_proceed(self, ecs, monkeypatch):
        monkeypatch.setenv("IFC_RUN__MONTH", "2026-08")
        ecs.write(a_row(last_modified_ts="2026-09-03 02:00:00.000"))
        ecs.run()
        assert ecs.built, "the reprocess did not accept the current month's recon"

    def test_a_run_month_row_is_not_enough(self, ecs, monkeypatch):
        monkeypatch.setenv("IFC_RUN__MONTH", "2026-08")
        ecs.write(a_row(last_modified_ts="2026-08-31 14:30:22.123"))
        summary = ecs.run()

        assert ecs.built == []
        assert summary["outcome"] == "UPSTREAM_PROCESSING_NOT_DONE"

    def test_the_weekend_skip_still_wins(self, main_ecs_script, monkeypatch, tmp_path,
                                         clean_ifc_env):
        """The recon gate runs after the run gate's skips on purpose: a weekend
        invocation is a day the run is not meant to happen, and checking
        upstream on it would alert six times a month for nothing."""
        import utility.run_gate as run_gate

        monkeypatch.setattr(run_gate, "today", lambda tz=None: date(2026, 9, 18))
        monkeypatch.setattr(
            main_ecs_script,
            "should_run",
            lambda *a, **k: GateOutcome(False, "2026-10-03 is a Saturday", True),
        )
        called = []
        monkeypatch.setattr(
            main_ecs_script, "_recon_gate", lambda *a, **k: called.append(True)
        )

        # No recon document anywhere: the gate would block if it were consulted.
        config = a_config(tmp_path)
        summary = main_ecs_script.ecs_handler(
            {"config_path": str(config), "trigger": "TRIGGER_8"}
        )

        assert summary["outcome"] == "SKIPPED"
        assert summary["exit_code"] == catalog.EXIT_OK
        assert called == [], "the recon gate must not run on a skipped invocation"


def _settings(**recon):
    from utility.connector_config import ConnectorSettings

    document = {
        "source": {"trigger_tables": {"TRIGGER_9": "ifc_trigger_db.t9"}},
        "kafka": {"topic": "t", "overrides": {"bootstrap.servers": "x:9092"}},
        "schema_registry": {"mode": "DEV"},
    }
    if recon:
        document["recon"] = recon
    return ConnectorSettings.model_validate(document)


class TestGateActivation:
    def test_it_is_off_when_no_table_is_configured(self):
        """Mirrors the run marker: unset means off, so a local run needs no feed."""
        settings = _settings()
        settings.select_trigger("TRIGGER_9")
        assert not settings.recon_active

    def test_a_table_and_a_target_for_the_trigger_turn_it_on(self):
        settings = _settings(table=TABLE, trigger_targets={"TRIGGER_9": TARGET_9})
        settings.select_trigger("TRIGGER_9")
        assert settings.recon_active

    def test_a_trigger_without_a_target_leaves_it_off(self):
        settings = _settings(table=TABLE, trigger_targets={"TRIGGER_8": TARGET_8})
        settings.select_trigger("TRIGGER_9")
        assert not settings.recon_active

    def test_enabled_false_wins_over_a_configured_table(self):
        settings = _settings(enabled=False, table=TABLE, trigger_targets={"TRIGGER_9": TARGET_9})
        settings.select_trigger("TRIGGER_9")
        assert not settings.recon_active

    def test_select_trigger_resolves_the_recon_target_too(self):
        """The trigger table read and the reconciliation checked must never
        belong to different triggers."""
        settings = _settings(table=TABLE, trigger_targets={"trigger 9": TARGET_9})
        settings.select_trigger("trigger 9")
        assert settings.source.table == "ifc_trigger_db.t9"
        assert settings.recon.target_table == TARGET_9

    def test_the_old_s3_recon_settings_fail_at_load(self):
        """An ignored key would leave the gate silently off."""
        with pytest.raises(Exception, match="trigger_paths"):
            _settings(trigger_paths={"TRIGGER_9": "s3://b/recon/trigger9/{MONTH}_{YYYY}/"})

    def test_the_recon_table_must_be_database_dot_table(self):
        with pytest.raises(Exception, match="database.table"):
            _settings(table="recon_audit")

    def test_the_bundled_config_resolves_every_trigger(self, clean_ifc_env):
        """One batch_recon table for every trigger; each trigger its own target."""
        from utility.connector_config import load_settings

        for trigger, suffix in (("TRIGGER_8", "8"), ("TRIGGER_9", "9"), ("TRIGGER_21", "21")):
            settings = load_settings("utility/connector_config.yaml")
            settings.select_trigger(trigger)
            assert settings.recon_active
            assert settings.recon.table == "bdb_ifc_synthetic_data_test.batch_recon"
            assert settings.recon.target_table == (
                f"`sit_cds_snsvc0080860_prepared_db`.bdb_ifc_synthetic_data_test.BDP_Corp_IFC_Trigger_{suffix}"
            )


# ---------------------------------------------------------------------------
# What each outcome records in the run marker table
# ---------------------------------------------------------------------------


class RecordingMarker:
    """Captures what the entry point writes, without a marker file."""

    def __init__(self):
        self.writes = []

    def is_done(self, trigger, month):
        return False

    def record(self, trigger, month, *, status, records=0, day=None, reason=""):
        self.writes.append({"trigger": trigger, "month": month,
                            "status": status, "records": records, "reason": reason})


@pytest.fixture
def recording(main_ecs_script, monkeypatch):
    marker = RecordingMarker()
    monkeypatch.setattr(main_ecs_script, "RunMarker", lambda *a, **k: marker)
    return marker


class TestTheRunMarkerStatus:
    def test_an_upstream_block_records_not_ran(self, ecs, recording, main_ecs_script,
                                               monkeypatch, tmp_path):
        """Weekend or upstream failure -> NOT RAN, with zero records."""
        from utility import run_gate

        ecs.write(a_row(status="RECON_FAILED"))
        config = a_config(tmp_path, marker=True)
        main_ecs_script.ecs_handler({"config_path": str(config), "trigger": "TRIGGER_8"})

        assert [w["status"] for w in recording.writes] == [run_gate.STATUS_NOT_RAN]
        assert recording.writes[0]["records"] == 0
        assert recording.writes[0]["month"] == "2026-09"

    def test_a_zero_count_month_records_success_with_zero_records(
        self, ecs, recording, main_ecs_script, tmp_path
    ):
        """A genuine empty month is delivered: SUCCESS closes it, so the rest of
        the window stands down instead of announcing it to TBB again."""
        from utility import run_gate

        ecs.write(a_row(source_count=0, target_count=0))
        config = a_config(tmp_path, marker=True)
        main_ecs_script.ecs_handler({"config_path": str(config), "trigger": "TRIGGER_8"})

        assert [(w["status"], w["records"]) for w in recording.writes] == [
            (run_gate.STATUS_SUCCESS, 0)
        ]
        assert recording.writes[0]["reason"] == "NO_DATA_THIS_MONTH"

    def test_a_delivered_month_records_not_ran(self, ecs, recording, main_ecs_script,
                                               monkeypatch, tmp_path):
        """Delivered on the 3rd, triggered again on the 4th: the 4th is
        reported as SKIPPED and recorded as NOT RAN, saying why."""
        from utility import run_gate

        monkeypatch.setattr(
            main_ecs_script, "should_run",
            lambda *a, **k: GateOutcome(False, "TRIGGER_8 2026-09 was already delivered", True),
        )
        config = a_config(tmp_path, marker=True)
        summary = main_ecs_script.ecs_handler({"config_path": str(config), "trigger": "TRIGGER_8"})

        assert summary["outcome"] == "SKIPPED"
        assert [w["status"] for w in recording.writes] == [run_gate.STATUS_NOT_RAN]
        assert recording.writes[0]["records"] == 0
        assert "already delivered" in recording.writes[0]["reason"]

    def test_a_failed_run_records_failure(self, ecs, recording, main_ecs_script, tmp_path):
        """The fused runner raises, which is a startup failure: the invocation
        ran and did not deliver."""
        from utility import run_gate

        ecs.write(a_row())
        config = a_config(tmp_path, marker=True)
        main_ecs_script.ecs_handler({"config_path": str(config), "trigger": "TRIGGER_8"})

        assert [w["status"] for w in recording.writes] == [run_gate.STATUS_FAILURE]
        assert recording.writes[0]["records"] == 0


# ---------------------------------------------------------------------------
# Every invocation leaves a manifest
# ---------------------------------------------------------------------------


@pytest.fixture
def manifests(monkeypatch):
    """Captures what would be written to the audit bucket's manifests/ folder."""
    from utility import audit_utility

    written = []
    monkeypatch.setattr(
        audit_utility.AuditWriter, "write_manifest", lambda self, manifest: written.append(manifest)
    )
    return written


class TestInvocationManifest:
    def test_a_recon_block_is_recorded(self, ecs, manifests):
        """The case that used to leave no trace: upstream said RECON_FAILED."""
        ecs.write(a_row(status="RECON_FAILED", error_record_count=17))
        summary = ecs.run()

        assert len(manifests) == 1
        manifest = manifests[0]
        assert manifest["stage"] == "RECON_GATE"
        assert manifest["outcome"] == "UPSTREAM_JOB_FAILED"
        assert manifest["exit_code"] == catalog.TED_JOB_FAILURE.exit_code
        assert manifest["failure"]["scenario"] == catalog.TED_JOB_FAILURE.key
        assert manifest["gate"]["recon"]["error_record_count"] == 17
        assert manifest["configuration"]["trigger"] == "TRIGGER_8"
        assert manifest["counters"]["published"] == 0
        assert summary["run_id"] == manifest["run_id"]

    def test_missing_upstream_data_is_recorded(self, ecs, manifests):
        ecs.run()
        assert [m["outcome"] for m in manifests] == ["UPSTREAM_DATA_NOT_RECEIVED"]

    def test_an_unreadable_recon_table_is_recorded(self, ecs, manifests, main_ecs_script,
                                                   monkeypatch):
        def broken(*a, **k):
            raise RuntimeError("access denied")

        monkeypatch.setattr(main_ecs_script.recon_gate, "evaluate", broken)
        ecs.run()

        assert len(manifests) == 1
        assert manifests[0]["outcome"] == "UPSTREAM_RECON_UNREADABLE"
        assert manifests[0]["reason"] == "access denied"

    def test_a_startup_failure_is_recorded(self, ecs, manifests):
        """The fused runner raises in its constructor, before run() is reached."""
        ecs.write(a_row())
        summary = ecs.run()

        assert len(manifests) == 1
        assert manifests[0]["stage"] == "STARTUP"
        assert manifests[0]["outcome"] == "FAILED"
        assert summary["run_id"] == manifests[0]["run_id"]

    def test_a_started_runner_is_not_recorded_twice(self, ecs, manifests, monkeypatch):
        """Once run() is entered the runner owns the manifest."""
        import utility.connector_runner as runner_module

        class Started:
            def __init__(self, *a, **k):
                pass

            def run(self):
                raise RuntimeError("after the runner's own manifest")

        monkeypatch.setattr(runner_module, "ConnectorRunner", Started)
        ecs.write(a_row())
        summary = ecs.run()

        assert manifests == []
        assert summary["run_id"] is None

    def test_a_run_gate_skip_is_recorded(self, main_ecs_script, monkeypatch, tmp_path,
                                         clean_ifc_env, manifests):
        import utility.run_gate as run_gate

        monkeypatch.setattr(run_gate, "today", lambda tz=None: date(2026, 9, 18))
        monkeypatch.setattr(
            main_ecs_script,
            "should_run",
            lambda *a, **k: GateOutcome(False, "2026-10-03 is a Saturday", True),
        )
        summary = main_ecs_script.ecs_handler(
            {"config_path": str(a_config(tmp_path)), "trigger": "TRIGGER_8"}
        )

        assert len(manifests) == 1
        assert manifests[0]["stage"] == "RUN_GATE"
        assert manifests[0]["outcome"] == "SKIPPED"
        assert manifests[0]["exit_code"] == catalog.EXIT_OK
        assert manifests[0]["reason"] == "2026-10-03 is a Saturday"
        assert summary["run_id"] == manifests[0]["run_id"]

    def test_a_run_gate_error_is_recorded_and_still_raised(self, main_ecs_script, monkeypatch,
                                                           tmp_path, clean_ifc_env, manifests):
        def unreachable(*a, **k):
            raise RuntimeError("marker table unreachable")

        monkeypatch.setattr(main_ecs_script, "should_run", unreachable)
        with pytest.raises(RuntimeError, match="unreachable"):
            main_ecs_script.ecs_handler(
                {"config_path": str(a_config(tmp_path)), "trigger": "TRIGGER_8"}
            )

        assert [(m["stage"], m["outcome"]) for m in manifests] == [("RUN_GATE", "FAILED")]
