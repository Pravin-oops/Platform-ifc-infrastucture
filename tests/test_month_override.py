"""Reprocessing a past month: ``IFC_RUN__MONTH``.

A one-off RunTask in October with ``IFC_RUN__MONTH=2026-08`` has to behave
exactly as the August run did - August's source and recon folders, July as the
business month, the outcome recorded against August - while the weekend check
and the marker's ``run_date`` stay on the day the task actually runs.
"""

from __future__ import annotations

import os
from datetime import date

import pytest
from pydantic import ValidationError

from utility import run_gate
from utility.connector_config import RunSettings, load_settings
from utility.run_gate import (
    RunMarker,
    execution_date,
    month_of,
    parse_month,
    set_execution_month,
    should_run,
)
from utility.tb_outcome_schema import EnvelopeBuilder
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

    def test_the_source_folder_is_augusts(self, settings):
        assert settings.source.resolved_path.endswith("/trigger_8/AUGUST_2026/")

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
        assert loaded.source.resolved_path.endswith("/trigger_8/OCTOBER_2026/")


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
