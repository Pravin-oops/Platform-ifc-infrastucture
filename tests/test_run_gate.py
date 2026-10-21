"""The entry-point gate.

Named after the scenarios the business walked through, so the behaviour can be
checked against the requirement without reading ``run_gate``.
"""

from __future__ import annotations

import io
import json
from datetime import date

import pytest
from botocore.exceptions import ClientError

from tests.conftest import use_config
from utility import run_gate
from utility.run_gate import (
    RunMarker,
    is_weekend,
    month_of,
    GateOutcome,
    STATUS_FAILURE,
    STATUS_NOT_RAN,
    STATUS_SUCCESS,
    should_run,
    today,
)

TRIGGER_8 = "TRIGGER_8"
MARKER_PATH = "s3://bucket/run-markers/run_markers.json"


def _client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class FakeS3:
    """Enough of S3 for the two calls the marker makes, conditions included.

    ``If-Match`` / ``If-None-Match: *`` are enforced the way S3 enforces them,
    because they are what stops two triggers finishing together from dropping
    each other's line - a fake that ignored them would hide exactly that bug.
    ``before_put`` lets a test slip another writer in between read and write.
    """

    def __init__(self):
        self.objects = {}
        self.calls = []
        self.before_put = None
        self._version = 0

    def body(self, path=MARKER_PATH):
        bucket, key = path[len("s3://"):].split("/", 1)
        return self.objects[(bucket, key)][0].decode("utf-8")

    def lines(self, path=MARKER_PATH):
        return [json.loads(line) for line in self.body(path).splitlines() if line]

    def get_object(self, **kwargs):
        self.calls.append(("get_object", kwargs))
        found = self.objects.get((kwargs["Bucket"], kwargs["Key"]))
        if found is None:
            raise _client_error("NoSuchKey", "GetObject")
        return {"Body": io.BytesIO(found[0]), "ETag": found[1]}

    def put_object(self, **kwargs):
        self.calls.append(("put_object", kwargs))
        if self.before_put is not None:
            hook, self.before_put = self.before_put, None
            hook(self)
        key = (kwargs["Bucket"], kwargs["Key"])
        current = self.objects.get(key)
        if kwargs.get("IfNoneMatch") == "*" and current is not None:
            raise _client_error("PreconditionFailed", "PutObject")
        if "IfMatch" in kwargs and (current is None or current[1] != kwargs["IfMatch"]):
            raise _client_error("PreconditionFailed", "PutObject")
        self._version += 1
        self.objects[key] = (kwargs["Body"], f'"etag-{self._version}"')

    def puts(self):
        return [kwargs for name, kwargs in self.calls if name == "put_object"]


@pytest.fixture(params=["s3", "local"])
def marker(request, tmp_path) -> RunMarker:
    """The gate's behaviour is the same whichever store the file lives in."""
    if request.param == "s3":
        return RunMarker(MARKER_PATH, client=FakeS3())
    return RunMarker(str(tmp_path / "run-markers" / "run_markers.json"))


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------


def test_weekend_detection():
    assert is_weekend(date(2026, 10, 3))  # Saturday
    assert is_weekend(date(2026, 10, 4))  # Sunday
    assert not is_weekend(date(2026, 10, 5))  # Monday


def test_execution_month_is_the_current_month():
    # A run on 3 September delivers September, not August.
    assert month_of(date(2026, 9, 3)) == "2026-09"


def test_today_is_a_date():
    assert isinstance(today(), date)


# ---------------------------------------------------------------------------
# The window
# ---------------------------------------------------------------------------


def test_third_is_saturday_so_the_window_lands_on_monday(marker):
    """3rd Sat -> skip, 4th Sun -> skip, 5th Mon -> process."""
    assert not should_run(TRIGGER_8, marker, day=date(2026, 10, 3))[0]
    assert not should_run(TRIGGER_8, marker, day=date(2026, 10, 4))[0]
    assert should_run(TRIGGER_8, marker, day=date(2026, 10, 5))[0]


def test_third_is_monday_so_the_later_days_stand_down(marker):
    """3rd Mon -> process; the 4th and 5th are weekdays but already delivered."""
    assert should_run(TRIGGER_8, marker, day=date(2026, 8, 3))[0]
    marker.mark_done(TRIGGER_8, "2026-08", records=412)

    for day in (date(2026, 8, 4), date(2026, 8, 5)):
        proceed, reason, _ = should_run(TRIGGER_8, marker, day=day)
        assert not proceed
        assert "already delivered" in reason


def test_each_trigger_has_its_own_marker(marker):
    marker.mark_done(TRIGGER_8, "2026-08", records=5)
    assert should_run("TRIGGER_9", marker, day=date(2026, 8, 10))[0]


def test_a_new_month_reopens_the_gate(marker):
    marker.mark_done(TRIGGER_8, "2026-08", records=9)
    assert should_run(TRIGGER_8, marker, day=date(2026, 9, 3))[0]


def test_an_undelivered_month_stays_open(marker):
    """The Databricks-failure case: no marker is written, so the window retries."""
    assert should_run(TRIGGER_8, marker, day=date(2026, 9, 3))[0]
    # The run found nothing, so nothing was marked.
    assert should_run(TRIGGER_8, marker, day=date(2026, 9, 4))[0]


# ---------------------------------------------------------------------------
# The marker file
# ---------------------------------------------------------------------------


def test_each_line_names_the_trigger_and_month_separately():
    """Separate columns, not one 'TRIGGER_8#2026-09' value, so Athena can
    filter on either on its own."""
    fake = FakeS3()
    RunMarker(MARKER_PATH, client=fake).mark_done(TRIGGER_8, "2026-09", records=412)

    (line,) = fake.lines()
    assert line["trigger_id"] == "TRIGGER_8"
    assert line["year_month"] == "2026-09"
    assert "pk" not in line


def test_the_line_records_what_was_delivered():
    fake = FakeS3()
    RunMarker(MARKER_PATH, client=fake).mark_done(TRIGGER_8, "2026-09", records=412)

    (line,) = fake.lines()
    assert line["run_status"] == "SUCCESS"
    assert line["records_processed"] == 412
    assert line["run_date"].startswith("20")
    assert line["recorded_at"].endswith("Z")
    assert line["reason"] == ""


def test_the_file_is_json_lines_that_athena_can_read():
    """Athena's JSON SerDe wants one object per line: no enclosing [ ], no
    pretty-printing, a newline after every record."""
    fake = FakeS3()
    m = RunMarker(MARKER_PATH, client=fake)
    m.mark_not_ran(TRIGGER_8, "2026-10", day=date(2026, 10, 3))
    m.mark_done(TRIGGER_8, "2026-10", records=3, day=date(2026, 10, 5))

    body = fake.body()
    assert body.endswith("\n")
    assert not body.lstrip().startswith("[")
    for line in body.splitlines():
        assert isinstance(json.loads(line), dict)
    assert fake.puts()[-1]["ContentType"] == "application/json"


def test_the_s3_location_is_passed_through():
    fake = FakeS3()
    RunMarker("s3://my-bucket/folder/run-markers/run_markers.json", client=fake).is_done(TRIGGER_8, "2026-09")
    assert fake.calls[0] == (
        "get_object",
        {"Bucket": "my-bucket", "Key": "folder/run-markers/run_markers.json"},
    )


def test_a_month_is_not_found_under_another_month(marker):
    """The month is part of the identity, not decoration."""
    marker.mark_done(TRIGGER_8, "2026-09", records=1)

    assert marker.is_done(TRIGGER_8, "2026-09")
    assert not marker.is_done(TRIGGER_8, "2026-10")
    assert not marker.is_done(TRIGGER_8, "2026-08")


def test_triggers_do_not_share_a_month(marker):
    marker.mark_done(TRIGGER_8, "2026-09", records=1)

    assert not marker.is_done("TRIGGER_9", "2026-09")
    assert not marker.is_done("TRIGGER_21", "2026-09")


def test_every_outcome_is_appended_with_its_status(marker):
    marker.mark_not_ran(TRIGGER_8, "2026-09", day=date(2026, 9, 5))
    marker.mark_failed(TRIGGER_8, "2026-09", day=date(2026, 9, 7))
    marker.mark_done(TRIGGER_8, "2026-09", records=150, day=date(2026, 9, 8))

    history = marker.history(TRIGGER_8, "2026-09")
    assert [h["run_status"] for h in history] == [STATUS_NOT_RAN, STATUS_FAILURE, STATUS_SUCCESS]
    assert [h["run_date"] for h in history] == ["2026-09-05", "2026-09-07", "2026-09-08"]
    # A history, not one row per month: the latest line is the month's state.
    assert marker.latest(TRIGGER_8, "2026-09")["records_processed"] == 150


def test_other_triggers_lines_are_kept(marker):
    """One file for every trigger: an append must not drop anyone else's line."""
    marker.mark_done(TRIGGER_8, "2026-09", records=1)
    marker.mark_done("TRIGGER_9", "2026-09", records=2)
    marker.mark_not_ran("TRIGGER_21", "2026-09")

    assert [(h["trigger_id"], h["run_status"]) for h in marker.history()] == [
        ("TRIGGER_8", STATUS_SUCCESS),
        ("TRIGGER_9", STATUS_SUCCESS),
        ("TRIGGER_21", STATUS_NOT_RAN),
    ]


def test_a_failed_forced_run_after_success_reopens_the_month(marker):
    """The latest line wins, so a forced re-delivery that fails leaves the
    month to be retried - as replacing the DynamoDB item used to."""
    marker.mark_done(TRIGGER_8, "2026-09", records=5)
    marker.mark_failed(TRIGGER_8, "2026-09")

    assert marker.status(TRIGGER_8, "2026-09") == STATUS_FAILURE
    assert not marker.is_done(TRIGGER_8, "2026-09")


def test_run_date_is_the_date_the_gate_looked(marker):
    """Not the month, and not when the records were produced."""
    marker.mark_done(TRIGGER_8, "2026-09", records=150, day=date(2026, 9, 3))
    assert marker.latest(TRIGGER_8, "2026-09")["run_date"] == "2026-09-03"


@pytest.mark.parametrize("status", [STATUS_NOT_RAN, STATUS_FAILURE])
def test_only_success_closes_the_month(marker, status):
    """A FAILURE or NOT RAN item means the month still has to be retried, so
    the presence of an item is not the question - its status is."""
    marker.record(TRIGGER_8, "2026-09", status=status)

    assert marker.status(TRIGGER_8, "2026-09") == status
    assert not marker.is_done(TRIGGER_8, "2026-09")
    assert should_run(TRIGGER_8, marker, day=date(2026, 9, 8)).proceed


def test_success_stops_the_next_invocation(marker):
    marker.mark_done(TRIGGER_8, "2026-09", records=150)

    assert marker.is_done(TRIGGER_8, "2026-09")
    outcome = should_run(TRIGGER_8, marker, day=date(2026, 9, 8))
    assert not outcome.proceed
    assert "already delivered" in outcome.reason


def test_a_delivered_month_keeps_its_success_line_untouched():
    """The later invocation appends; it never rewrites the SUCCESS line."""
    fake = FakeS3()
    marker = RunMarker(MARKER_PATH, client=fake)
    marker.mark_done(TRIGGER_8, "2026-09", records=150, day=date(2026, 9, 3))
    body_before = fake.body()

    outcome = should_run(TRIGGER_8, marker, day=date(2026, 9, 4))
    assert not outcome.proceed
    assert outcome.mark_not_ran
    marker.mark_not_ran(TRIGGER_8, "2026-09", day=date(2026, 9, 4), reason=outcome.reason)

    assert fake.body().startswith(body_before)
    success, not_ran = fake.lines()
    assert (success["run_date"], success["run_status"], success["records_processed"]) == (
        "2026-09-03", STATUS_SUCCESS, 150)
    assert (not_ran["run_date"], not_ran["run_status"]) == ("2026-09-04", STATUS_NOT_RAN)


def test_success_on_the_3rd_then_a_wrong_trigger_on_the_4th(marker):
    """Delivered on the 3rd; the 4th is triggered anyway. The 4th records
    NOT RAN (already delivered), the month stays SUCCESS, and the 5th stands
    down the same way rather than delivering it again."""
    marker.mark_done(TRIGGER_8, "2026-08", records=412, day=date(2026, 8, 3))

    for day in (date(2026, 8, 4), date(2026, 8, 5)):
        outcome = should_run(TRIGGER_8, marker, day=day)
        assert not outcome.proceed
        assert outcome.mark_not_ran
        marker.mark_not_ran(TRIGGER_8, "2026-08", day=day, reason=outcome.reason)

        assert marker.status(TRIGGER_8, "2026-08") == STATUS_SUCCESS
        assert marker.is_done(TRIGGER_8, "2026-08")

    history = marker.history(TRIGGER_8, "2026-08")
    assert [(h["run_date"], h["run_status"]) for h in history] == [
        ("2026-08-03", STATUS_SUCCESS),
        ("2026-08-04", STATUS_NOT_RAN),
        ("2026-08-05", STATUS_NOT_RAN),
    ]
    assert "already delivered" in history[1]["reason"]
    assert history[1]["records_processed"] == 0


def test_not_ran_does_not_close_a_failed_month(marker):
    """NOT RAN never changes the outcome either way: after a FAILURE, a
    weekend NOT RAN leaves the month open for the next weekday."""
    marker.mark_failed(TRIGGER_8, "2026-10", day=date(2026, 10, 2))
    marker.mark_not_ran(TRIGGER_8, "2026-10", day=date(2026, 10, 3))

    assert marker.status(TRIGGER_8, "2026-10") == STATUS_FAILURE
    assert should_run(TRIGGER_8, marker, day=date(2026, 10, 5)).proceed


def test_a_weekend_asks_for_a_not_ran_marker(marker):
    outcome = should_run(TRIGGER_8, marker, day=date(2026, 10, 3))
    assert not outcome.proceed
    assert outcome.mark_not_ran


def test_a_delivered_month_on_a_weekend_says_already_delivered(marker):
    """SUCCESS is checked ahead of the weekend, so the Saturday after a
    successful Friday records the reason that matters - and stays delivered."""
    marker.mark_done(TRIGGER_8, "2026-10", records=150, day=date(2026, 10, 2))

    outcome = should_run(TRIGGER_8, marker, day=date(2026, 10, 3))
    assert not outcome.proceed
    assert outcome.mark_not_ran
    assert "already delivered" in outcome.reason
    marker.mark_not_ran(TRIGGER_8, "2026-10", day=date(2026, 10, 3), reason=outcome.reason)
    assert marker.is_done(TRIGGER_8, "2026-10")


def test_records_processed_is_zero_for_anything_but_success(marker):
    marker.mark_not_ran(TRIGGER_8, "2026-09")
    marker.mark_failed(TRIGGER_8, "2026-09")

    assert [h["records_processed"] for h in marker.history()] == [0, 0]


def test_a_month_never_attempted_has_no_status(marker):
    assert marker.status(TRIGGER_8, "2026-09") is None
    assert not marker.is_done(TRIGGER_8, "2026-09")


# ---------------------------------------------------------------------------
# Appending to S3, which has no append
# ---------------------------------------------------------------------------


def test_the_first_write_creates_the_file_only_if_it_is_still_absent():
    fake = FakeS3()
    RunMarker(MARKER_PATH, client=fake).mark_not_ran(TRIGGER_8, "2026-10")

    (put,) = fake.puts()
    assert put["IfNoneMatch"] == "*"
    assert "IfMatch" not in put


def test_an_append_is_conditional_on_the_version_it_read():
    fake = FakeS3()
    m = RunMarker(MARKER_PATH, client=fake)
    m.mark_not_ran(TRIGGER_8, "2026-10")
    etag = fake.objects[("bucket", "run-markers/run_markers.json")][1]

    m.mark_done(TRIGGER_8, "2026-10", records=1)

    assert fake.puts()[-1]["IfMatch"] == etag
    assert "IfNoneMatch" not in fake.puts()[-1]


def test_a_concurrent_append_is_retried_not_lost():
    """TRIGGER_8 and TRIGGER_9 finishing together: both lines survive."""
    fake = FakeS3()
    m = RunMarker(MARKER_PATH, client=fake)
    m.mark_done("TRIGGER_9", "2026-09", records=2)

    other = RunMarker(MARKER_PATH, client=fake)
    fake.before_put = lambda _: other.mark_not_ran("TRIGGER_21", "2026-09")
    m.mark_done(TRIGGER_8, "2026-09", records=1)

    assert sorted(h["trigger_id"] for h in m.history()) == ["TRIGGER_21", "TRIGGER_8", "TRIGGER_9"]


def test_two_writers_creating_the_file_together_both_land():
    fake = FakeS3()
    m = RunMarker(MARKER_PATH, client=fake)
    other = RunMarker(MARKER_PATH, client=fake)
    fake.before_put = lambda _: other.mark_not_ran("TRIGGER_9", "2026-10")

    m.mark_not_ran(TRIGGER_8, "2026-10")

    assert [h["trigger_id"] for h in m.history()] == ["TRIGGER_9", TRIGGER_8]


def test_a_conflict_that_never_clears_is_raised(monkeypatch):
    """Bounded: the caller's best-effort handler logs it loudly."""
    fake = FakeS3()
    m = RunMarker(MARKER_PATH, client=fake)
    m.mark_not_ran(TRIGGER_8, "2026-10")

    def always_conflict(**kwargs):
        fake.calls.append(("put_object", kwargs))
        raise _client_error("PreconditionFailed", "PutObject")

    monkeypatch.setattr(fake, "put_object", always_conflict)
    with pytest.raises(ClientError):
        m.mark_done(TRIGGER_8, "2026-10", records=1)
    assert len(fake.puts()) == 1 + run_gate.APPEND_ATTEMPTS


def test_a_read_that_is_not_a_missing_file_is_raised():
    """AccessDenied must not read as 'never attempted', or a delivered month
    would run again. (Without s3:ListBucket a missing key also comes back as
    AccessDenied, which is why the task role grants it.)"""

    class Denied(FakeS3):
        def get_object(self, **kwargs):
            raise _client_error("AccessDenied", "GetObject")

    with pytest.raises(ClientError):
        RunMarker(MARKER_PATH, client=Denied()).is_done(TRIGGER_8, "2026-09")


def test_a_corrupt_line_is_an_error_not_a_skip():
    """The skipped line might be the SUCCESS: skipping it would republish."""
    fake = FakeS3()
    fake.objects[("bucket", "run-markers/run_markers.json")] = (b'{"trigger_id": "TRIGGER_8",\n', '"e"')

    with pytest.raises(ValueError, match="line 1"):
        RunMarker(MARKER_PATH, client=fake).is_done(TRIGGER_8, "2026-09")


def test_a_file_without_a_final_newline_is_not_glued_onto(tmp_path):
    path = tmp_path / "run_markers.json"
    path.write_text(json.dumps({"trigger_id": "TRIGGER_9", "year_month": "2026-09",
                                "run_status": "SUCCESS"}), encoding="utf-8")

    m = RunMarker(str(path))
    m.mark_done(TRIGGER_8, "2026-09", records=1)

    assert [h["trigger_id"] for h in m.history()] == ["TRIGGER_9", TRIGGER_8]
    assert m.is_done("TRIGGER_9", "2026-09")


def test_a_local_file_and_its_folder_are_created(tmp_path):
    path = tmp_path / "nested" / "run_markers.json"
    RunMarker(str(path)).mark_not_ran(TRIGGER_8, "2026-10")
    assert path.read_text(encoding="utf-8").count("\n") == 1


# ---------------------------------------------------------------------------
# Force
# ---------------------------------------------------------------------------


def test_force_overrides_a_delivered_month(marker):
    marker.mark_done(TRIGGER_8, "2026-08", records=7)
    assert should_run(TRIGGER_8, marker, day=date(2026, 8, 4), force=True)[0]


def test_force_overrides_the_weekend(marker):
    assert should_run(TRIGGER_8, marker, day=date(2026, 10, 3), force=True)[0]


# ---------------------------------------------------------------------------
# Through the ECS entry point
# ---------------------------------------------------------------------------


def test_a_weekend_invocation_exits_clean_without_touching_anything(
    main_ecs_script, monkeypatch, tmp_path, clean_ifc_env
):
    """A weekend start costs one container start: no Kafka, no source, exit 0."""
    config = tmp_path / "c.yaml"
    config.write_text(
        "\n".join(
            [
                "app: {name: t, environment: TEST}",
                "source: {table: ifc_trigger_db.trigger_8}",
                "kafka: {topic: t, overrides: {bootstrap.servers: 'localhost:9092'}}",
                "schema_registry: {mode: DEV}",
                f"run_marker: {{path: '{tmp_path / 'run_markers.json'}'}}",
                "health: {enabled: false}",
            ]
        ),
        encoding="utf-8",
    )

    import utility.connector_runner as runner_module

    class Boom:
        def __init__(self, *a, **k):
            raise AssertionError("the runner must not be built on a weekend")

    monkeypatch.setattr(runner_module, "ConnectorRunner", Boom)
    monkeypatch.setattr(
        main_ecs_script, "should_run", lambda *a, **k: GateOutcome(False, "2026-10-03 is a Saturday", True)
    )

    use_config(monkeypatch, main_ecs_script, config)
    result = main_ecs_script.ecs_handler({"trigger": TRIGGER_8})

    assert result["exit_code"] == 0
    assert result["outcome"] == "SKIPPED"
    # The weekend is on the record as NOT RAN.
    (line,) = RunMarker(str(tmp_path / "run_markers.json")).history()
    assert (line["trigger_id"], line["run_status"]) == (TRIGGER_8, STATUS_NOT_RAN)


def test_gating_is_off_without_a_marker_file(tmp_path, clean_ifc_env):
    """Without a marker file the entry-point gate is off."""
    from utility.connector_config import ConnectorSettings

    settings = ConnectorSettings.model_validate(
        {
            "source": {"table": "ifc_trigger_db.trigger_8"},
            "kafka": {"topic": "t", "overrides": {"bootstrap.servers": "localhost:9092"}},
            "schema_registry": {"mode": "DEV"},
        }
    )
    assert settings.gate_active is False


def test_a_leftover_dynamodb_table_name_is_rejected(tmp_path, clean_ifc_env):
    """Ignoring it would leave the path unset, switch the gate off, and let
    every date in the window republish the month."""
    from pydantic import ValidationError

    from utility.connector_config import ConnectorSettings

    with pytest.raises(ValidationError, match="run_marker.path"):
        ConnectorSettings.model_validate(
            {
                "source": {"table": "ifc_trigger_db.trigger_8"},
                "kafka": {"topic": "t", "overrides": {"bootstrap.servers": "localhost:9092"}},
                "schema_registry": {"mode": "DEV"},
                "run_marker": {"table_name": "ifc-trigger-connector-run-markers"},
            }
        )


# ---------------------------------------------------------------------------
# The scheduler names the trigger; the trigger picks the Athena table
# ---------------------------------------------------------------------------


def _settings(**source):
    from utility.connector_config import ConnectorSettings

    return ConnectorSettings.model_validate(
        {
            "source": source,
            "kafka": {"topic": "t", "overrides": {"bootstrap.servers": "localhost:9092"}},
            "schema_registry": {"mode": "DEV"},
            "run_marker": {"path": MARKER_PATH},
        }
    )


TRIGGER_TABLES = {
    "TRIGGER_8": "ifc_trigger_db.trigger_8",
    "trigger 9": "ifc_trigger_db.trigger_9",
}


@pytest.mark.parametrize("spelling", ["trigger 9", "Trigger 9", "TRIGGER_9", "trigger-9", "9", 9])
def test_scheduler_spellings_resolve_to_one_trigger(spelling, clean_ifc_env):
    settings = _settings(trigger_tables=TRIGGER_TABLES)
    assert settings.select_trigger(spelling) == "TRIGGER_9"
    assert settings.source.table == "ifc_trigger_db.trigger_9"


def test_one_marker_whatever_the_spelling(marker):
    """'trigger 9' and 'TRIGGER_9' must not each deliver the same month."""
    from utility.connector_config import canonical_trigger

    marker.mark_done(canonical_trigger("trigger 9"), "2026-09", records=3)
    assert not should_run(canonical_trigger("TRIGGER_9"), marker, day=date(2026, 9, 8))[0]


def test_an_unknown_trigger_is_rejected(clean_ifc_env):
    with pytest.raises(ValueError):
        _settings(trigger_tables=TRIGGER_TABLES).select_trigger("trigger 99")


def test_an_unmapped_trigger_without_a_fallback_fails(clean_ifc_env):
    with pytest.raises(ValueError, match="No Athena table"):
        _settings(trigger_tables=TRIGGER_TABLES).select_trigger("TRIGGER_21")


def test_a_trigger_table_beats_the_fallback_table(clean_ifc_env):
    settings = _settings(table="ifc_trigger_db.fallback", trigger_tables=TRIGGER_TABLES)
    settings.select_trigger("TRIGGER_21")
    assert settings.source.table == "ifc_trigger_db.fallback"

    settings = _settings(table="ifc_trigger_db.fallback", trigger_tables=TRIGGER_TABLES)
    settings.select_trigger("TRIGGER_8")
    assert settings.source.table == "ifc_trigger_db.trigger_8"


def test_a_run_without_a_trigger_is_refused(clean_ifc_env):
    """The table holds only attributes, so the trigger is what names the sub-type."""
    with pytest.raises(ValueError, match="No trigger specified"):
        _settings(table="ifc_trigger_db.fallback").select_trigger(None)


def test_env_trigger_is_normalised(monkeypatch, clean_ifc_env):
    monkeypatch.setenv("IFC_RUN__TRIGGER", "trigger 9")
    from utility.connector_config import _deep_merge, _env_overlay, ConnectorSettings

    doc = {
        "source": {"trigger_tables": TRIGGER_TABLES},
        "kafka": {"topic": "t", "overrides": {"bootstrap.servers": "localhost:9092"}},
        "schema_registry": {"mode": "DEV"},
    }
    settings = ConnectorSettings.model_validate(_deep_merge(doc, _env_overlay()))
    settings.select_trigger(None)
    assert settings.run.trigger == "TRIGGER_9"
    assert settings.source.table == "ifc_trigger_db.trigger_9"


@pytest.mark.parametrize(
    "argv", [["trigger", "9"], ["trigger 9"], ["--trigger", "TRIGGER_9"]]
)
def test_command_line_trigger(main_ecs_script, argv):
    from utility.connector_config import canonical_trigger

    assert canonical_trigger(main_ecs_script._event_from_argv(argv)["trigger"]) == "TRIGGER_9"


def test_command_line_is_optional(main_ecs_script):
    assert main_ecs_script._event_from_argv([]) == {}


def test_trigger_9_invocation_reads_trigger_9_location(
    main_ecs_script, monkeypatch, tmp_path, clean_ifc_env
):
    """Not yet delivered this month -> runs against TRIGGER_9's table."""
    config = tmp_path / "c.yaml"
    config.write_text(
        "\n".join(
            [
                "app: {name: t, environment: TEST}",
                "source: {trigger_tables: {TRIGGER_8: ifc_trigger_db.t8, TRIGGER_9: ifc_trigger_db.t9}}",
                "kafka: {topic: t, overrides: {bootstrap.servers: 'localhost:9092'}}",
                "schema_registry: {mode: DEV}",
                f"run_marker: {{path: '{MARKER_PATH}'}}",
                "health: {enabled: false}",
            ]
        ),
        encoding="utf-8",
    )

    import utility.connector_runner as runner_module

    seen = {}

    class Runner:
        run_id = "r1"
        last_result = None

        def __init__(self, settings, **_):
            seen["table"] = settings.source.table

        def run(self):
            return 0

    fake_marker = RunMarker(MARKER_PATH, client=FakeS3())
    monkeypatch.setattr(runner_module, "ConnectorRunner", Runner)
    monkeypatch.setattr(main_ecs_script, "RunMarker", lambda *_a, **_k: fake_marker)
    monkeypatch.setattr(
        main_ecs_script, "should_run",
        lambda t, m, **k: GateOutcome(not m.is_done(t, "2026-09"), "x", m.is_done(t, "2026-09")),
    )

    use_config(monkeypatch, main_ecs_script, config)
    result = main_ecs_script.ecs_handler({"trigger": "trigger 9"})
    assert result["exit_code"] == 0
    assert seen["table"] == "ifc_trigger_db.t9"

    # Delivered this month -> the next invocation stands down without reading,
    # and records that it looked: NOT RAN, with the SUCCESS still in force.
    fake_marker.mark_done("TRIGGER_9", "2026-09", records=1)
    seen.clear()
    use_config(monkeypatch, main_ecs_script, config)
    result = main_ecs_script.ecs_handler({"trigger": "trigger 9"})
    assert result["outcome"] == "SKIPPED"
    assert seen == {}
    assert fake_marker.history("TRIGGER_9")[-1]["run_status"] == STATUS_NOT_RAN
    assert fake_marker.is_done("TRIGGER_9", "2026-09")


# ---------------------------------------------------------------------------
# The DevOps contract: EventBridge Scheduler sets IFC_RUN__TRIGGER=TRIGGER_9
# ---------------------------------------------------------------------------


def _settings_from_env():
    from utility.connector_config import ConnectorSettings, _deep_merge, _env_overlay

    doc = {
        "source": {"trigger_tables": TRIGGER_TABLES},
        "kafka": {"topic": "t", "overrides": {"bootstrap.servers": "localhost:9092"}},
        "schema_registry": {"mode": "DEV"},
    }
    return ConnectorSettings.model_validate(_deep_merge(doc, _env_overlay()))


@pytest.mark.parametrize("value", ["TRIGGER_9", "9", " trigger 9 "])
def test_scheduler_env_variable_selects_the_trigger(value, monkeypatch, clean_ifc_env):
    monkeypatch.setenv("IFC_RUN__TRIGGER", value)
    settings = _settings_from_env()
    settings.select_trigger(None)
    assert settings.run.trigger == "TRIGGER_9"
    assert settings.source.table == "ifc_trigger_db.trigger_9"


def test_scheduler_env_variable_through_the_ecs_entry_point(
    main_ecs_script, monkeypatch, tmp_path, clean_ifc_env
):
    """No command argument, only the environment - exactly what the scheduler sends."""
    config = tmp_path / "c.yaml"
    config.write_text(
        "\n".join(
            [
                "source: {trigger_tables: {TRIGGER_8: ifc_trigger_db.t8, TRIGGER_9: ifc_trigger_db.t9}}",
                "kafka: {topic: t, overrides: {bootstrap.servers: 'localhost:9092'}}",
                "schema_registry: {mode: DEV}",
                f"run_marker: {{path: '{MARKER_PATH}'}}",
                "health: {enabled: false}",
            ]
        ),
        encoding="utf-8",
    )
    use_config(monkeypatch, main_ecs_script, config)
    monkeypatch.setenv("IFC_RUN__TRIGGER", "TRIGGER_9")

    import utility.connector_runner as runner_module

    seen = {}

    class Runner:
        run_id = "r1"
        last_result = None

        def __init__(self, settings, **_):
            seen["trigger"], seen["table"] = settings.run.trigger, settings.source.table

        def run(self):
            return 0

    gate_calls = []
    monkeypatch.setattr(runner_module, "ConnectorRunner", Runner)
    monkeypatch.setattr(main_ecs_script, "RunMarker", lambda *_a, **_k: RunMarker(MARKER_PATH, client=FakeS3()))
    monkeypatch.setattr(
        main_ecs_script, "should_run", lambda t, m, **k: GateOutcome(gate_calls.append(t) or True, "due", False)
    )

    assert main_ecs_script.main([]) == 0
    assert gate_calls == ["TRIGGER_9"]
    assert seen == {"trigger": "TRIGGER_9", "table": "ifc_trigger_db.t9"}
