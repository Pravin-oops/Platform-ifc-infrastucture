"""Reprocessing a past month: ``IFC_RUN__MONTH``.

A one-off RunTask in October with ``IFC_RUN__MONTH=2026-08`` has to behave
exactly as the August run did - July's rows (``business_date = 2026-07-31``),
July as the business month, the outcome recorded against August - while the
weekend check, the marker's ``run_date`` and the recon folder stay on the day
the task actually runs: upstream lands the recon document in the current
month's folder.
"""

from __future__ import annotations

import io
import os
from datetime import date

import pytest
from fastavro import parse_schema, schemaless_reader
from pydantic import ValidationError

from tests.test_runner_pipeline import FakeAthena, FakeProducer, VALID_ROW, make_settings
from utility import run_gate
from utility.connector_config import RunSettings, load_settings
from utility.connector_runner import ConnectorRunner
from utility.recon_gate import ReconSource
from utility.resilience_utility import ShutdownSignal
from utility.run_gate import (
    RunMarker,
    execution_date,
    month_of,
    parse_month,
    set_execution_month,
    should_run,
)
from utility.tb_outcome_schema import EnvelopeBuilder
from utility.trigger_source import make_source
from utility.connector_utility import load_schema_document

TRIGGER_8 = "TRIGGER_8"
#: A Thursday in October: neither a weekend nor August.
OCTOBER_1 = date(2026, 10, 1)


@pytest.fixture
def in_october(monkeypatch):
    monkeypatch.setattr(run_gate, "today", lambda tz=None: OCTOBER_1)


@pytest.fixture
def marker(tmp_path):
    return RunMarker(str(tmp_path / "run_markers.json"))


class TestParsing:
    @pytest.mark.parametrize(
        "value", ["2026-08", "2026-8", "AUGUST_2026", "August 2026", "august-2026", " 2026-08 "]
    )
    def test_every_spelling_is_the_first_of_august(self, value):
        assert parse_month(value) == date(2026, 8, 1)

    @pytest.mark.parametrize("value", ["2026-13", "2026-00", "AUGUSTO_2026", "08-2026", "202608", "August"])
    def test_a_malformed_month_is_refused(self, value):
        with pytest.raises(ValueError):
            parse_month(value)

    def test_the_setting_is_normalised_to_yyyy_mm(self):
        assert RunSettings(month="AUGUST_2026").month == "2026-08"
        assert RunSettings(month="").month is None
        assert RunSettings().month is None

    def test_a_bad_setting_fails_the_config_not_the_run(self):
        with pytest.raises(ValidationError):
            RunSettings(month="Augst 2026")


class TestExecutionDate:
    def test_unset_it_is_today(self, in_october):
        assert execution_date() == OCTOBER_1

    def test_set_it_is_the_first_of_that_month(self, in_october):
        set_execution_month("2026-08")
        assert execution_date() == date(2026, 8, 1)

        set_execution_month(None)
        assert execution_date() == OCTOBER_1


class TestLoadedFromTheEnvironment:
    @pytest.fixture
    def settings(self, app_root, clean_ifc_env, in_october, monkeypatch):
        monkeypatch.setenv("IFC_RUN__MONTH", "2026-08")
        loaded = load_settings(os.path.join(app_root, "utility", "connector_config.yaml"))
        loaded.select_trigger(TRIGGER_8)
        return loaded

    def test_the_setting_carries_the_month(self, settings):
        assert settings.run.month == "2026-08"

    def test_the_query_reads_the_rows_the_august_run_read(self, settings):
        source = make_source(settings.source, trigger=settings.trigger)
        assert source.business_date == date(2026, 7, 31)
        assert source.query()[1] == ["'2026-07-31'"]

    def test_the_recon_folder_is_the_current_months(self, settings):
        assert ReconSource(settings.recon).folder.endswith("/trigger8/OCTOBER_2026/")

    def test_the_records_are_stamped_as_the_august_run_stamped_them(self, settings):
        builder = EnvelopeBuilder(avro_schema=load_schema_document("utility/schema.json"))
        assert builder.business_month == "2026-07"
        assert builder.event_timestamp == "2026-07-31T23:59:59.999999999Z"

    def test_the_gate_month_is_august(self, settings):
        assert month_of(execution_date()) == "2026-08"

    def test_without_it_october_is_processed(self, app_root, clean_ifc_env, in_october):
        loaded = load_settings(os.path.join(app_root, "utility", "connector_config.yaml"))
        loaded.select_trigger(TRIGGER_8)
        assert loaded.run.month is None
        assert make_source(loaded.source, trigger=loaded.trigger).business_date == date(2026, 9, 30)
        assert ReconSource(loaded.recon).folder.endswith("/trigger8/OCTOBER_2026/")


class TestTheQueriedBusinessDate:
    """``IFC_RUN__MONTH`` names the run month; the query reads the last day of
    the month before it, as that month's scheduled run would have."""

    @pytest.mark.parametrize(
        "run_month, business_date",
        [
            ("2026-09", "2026-08-31"),
            ("SEPTEMBER_2026", "2026-08-31"),
            ("2026-10", "2026-09-30"),
            ("2027-01", "2026-12-31"),
            ("2028-03", "2028-02-29"),
        ],
    )
    def test_the_query_reads_the_previous_month_end(
        self, app_root, clean_ifc_env, in_october, monkeypatch, run_month, business_date
    ):
        monkeypatch.setenv("IFC_RUN__MONTH", run_month)
        loaded = load_settings(os.path.join(app_root, "utility", "connector_config.yaml"))
        loaded.select_trigger(TRIGGER_8)

        source = make_source(loaded.source, trigger=loaded.trigger)
        assert source.query()[1] == [f"'{business_date}'"]


class TestTheRun:
    """Through the runner: the rows queried and the month stamped on them agree."""

    def test_a_reprocess_queries_and_stamps_july(self, in_october):
        set_execution_month("2026-08")
        settings = make_settings(
            schema_registry={"mode": "DEV", "schema_id": 1299},
            kafka={"bsp_config_path": None, "overrides": {"bootstrap.servers": "localhost:9092"}},
        )
        athena = FakeAthena([dict(VALID_ROW, business_date="2026-07-31")])
        producer = FakeProducer()
        runner = ConnectorRunner(settings, shutdown=ShutdownSignal(), producer_factory=lambda _: producer)
        runner._source._client = athena
        runner._source._sleep = lambda _s: None

        runner.start()
        result = runner.run_batch()

        assert result.counters.published == 1
        assert athena.queries[0]["ExecutionParameters"] == ["'2026-07-31'"]
        assert result.source["business_date"] == "2026-07-31"

        schema = load_schema_document("utility/schema.json")
        body = schemaless_reader(io.BytesIO(producer.produced[0]["value"][5:]), parse_schema(schema))
        assert body["timestamp"] == "2026-07-31T23:59:59.999999999Z"


class TestTheGate:
    def test_an_undelivered_past_month_runs(self, marker):
        marker.mark_failed(TRIGGER_8, "2026-08")
        assert should_run(TRIGGER_8, marker, day=OCTOBER_1, month="2026-08").proceed

    def test_a_delivered_past_month_still_needs_force(self, marker):
        marker.mark_done(TRIGGER_8, "2026-08", records=412)

        outcome = should_run(TRIGGER_8, marker, day=OCTOBER_1, month="2026-08")
        assert not outcome.proceed
        assert "2026-08 was already delivered" in outcome.reason

        assert should_run(TRIGGER_8, marker, day=OCTOBER_1, month="2026-08", force=True).proceed

    def test_the_weekend_is_the_day_the_task_runs_not_the_month(self, marker):
        # 1 August 2026 is a Saturday; 1 October is a Thursday.
        assert should_run(TRIGGER_8, marker, day=OCTOBER_1, month="2026-08").proceed
        assert not should_run(TRIGGER_8, marker, day=date(2026, 10, 3), month="2026-08").proceed

    def test_the_marker_records_augusts_month_on_octobers_date(self, marker, in_october):
        marker.mark_done(TRIGGER_8, "2026-08", records=7)

        line = marker.latest(TRIGGER_8, "2026-08")
        assert line["year_month"] == "2026-08"
        assert line["run_date"] == "2026-10-01"
