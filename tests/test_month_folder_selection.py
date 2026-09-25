"""Finding this month's extract without republishing the config.

TED writes one folder per month - ``trigger_8/SEPTEMBER_2026/`` - holding
timestamped extracts named ``trigger8_YYYYMMDD_HHMMSS_ffffff.json``. Two things
have to happen at run time: the folder name has to come from the run date, and
the newest extract in it has to win, because TED rewrites the month rather than
appending and every older file beside it is a superseded draft.

The date substituted is the *run* month, which is what the run gate keys on -
not the business month the records carry. A September run reads SEPTEMBER_2026
and publishes records stamped with August.
"""

from __future__ import annotations

import json
from datetime import date, datetime

import pytest

from ifc_trigger_connector.utility.connector_config import (
    SourceSettings,
    expand_date_tokens,
)
from ifc_trigger_connector.utility.trigger_source import (
    TriggerSource,
    parse_filename_timestamp,
)

PATTERN = r"(\d{8}_\d{6}_\d+)"
FORMAT = "%Y%m%d_%H%M%S_%f"


@pytest.fixture
def run_date(monkeypatch):
    """Pin the run date so folder expansion is deterministic."""
    def pin(day):
        import ifc_trigger_connector.utility.run_gate as run_gate

        monkeypatch.setattr(run_gate, "today", lambda tz=None: day)
        return day

    return pin


def write_extract(folder, name, tag):
    folder.mkdir(parents=True, exist_ok=True)
    (folder / name).write_text(
        json.dumps(
            [
                {
                    "triggerType": "IFC_CDD",
                    "triggerSubType": "TRIGGER_8",
                    "attributes": {
                        "counterparty_full_legal_entity_name": tag,
                        "counterparty_csid_sds": 9912345678,
                        "region": "EMEA",
                        "business_date": "2026-08-31",
                    },
                }
            ]
        ),
        encoding="utf-8",
    )
    return folder / name


def tags_read(source):
    return [i.attributes["counterparty_full_legal_entity_name"] for i in source.stream()]


class TestDateTokens:
    def test_the_month_folder_comes_from_the_run_date(self):
        template = "s3://location/trigger_8/{MONTH}_{YYYY}/"
        assert (
            expand_date_tokens(template, date(2026, 9, 15))
            == "s3://location/trigger_8/SEPTEMBER_2026/"
        )

    @pytest.mark.parametrize(
        "day, expected",
        [
            (date(2026, 1, 31), "JANUARY_2026"),
            (date(2026, 9, 1), "SEPTEMBER_2026"),
            (date(2026, 12, 31), "DECEMBER_2026"),
            (date(2027, 1, 1), "JANUARY_2027"),
        ],
    )
    def test_every_month_renders_in_the_upper_case_form_ted_writes(self, day, expected):
        assert expand_date_tokens("{MONTH}_{YYYY}", day) == expected

    def test_the_other_tokens_render(self):
        day = date(2026, 9, 5)
        assert expand_date_tokens("{YYYYMM}", day) == "202609"
        assert expand_date_tokens("{YYYY-MM}", day) == "2026-09"
        assert expand_date_tokens("{YYYYMMDD}", day) == "20260905"
        assert expand_date_tokens("{MM}/{DD}", day) == "09/05"

    def test_a_path_without_tokens_is_left_alone(self):
        plain = "s3://location/trigger_8/"
        assert expand_date_tokens(plain, date(2026, 9, 15)) == plain

    def test_the_settings_expand_on_every_read(self, run_date):
        """Expansion is lazy, so a resident service crossing a month boundary
        moves to the new folder without a restart."""
        settings = SourceSettings(type="s3", path="s3://loc/trigger_8/{MONTH}_{YYYY}/")

        run_date(date(2026, 9, 30))
        assert settings.resolved_path.endswith("/SEPTEMBER_2026/")

        run_date(date(2026, 10, 1))
        assert settings.resolved_path.endswith("/OCTOBER_2026/")

    def test_the_raw_template_is_kept_not_overwritten(self, run_date):
        run_date(date(2026, 9, 15))
        settings = SourceSettings(type="s3", path="s3://loc/trigger_8/{MONTH}_{YYYY}/")
        settings.resolved_path
        assert "{MONTH}" in settings.path


class TestFilenameTimestamp:
    def test_the_ted_filename_parses(self):
        stamp = parse_filename_timestamp(
            "trigger8_20260930_143022_123456.json", pattern=PATTERN, fmt=FORMAT
        )
        assert stamp == datetime(2026, 9, 30, 14, 30, 22, 123456)

    def test_a_name_without_a_timestamp_is_none_not_an_error(self):
        assert parse_filename_timestamp("manual_upload.json", pattern=PATTERN, fmt=FORMAT) is None

    def test_a_matching_but_impossible_date_is_none(self):
        assert (
            parse_filename_timestamp(
                "trigger8_20261331_143022_123456.json", pattern=PATTERN, fmt=FORMAT
            )
            is None
        )


class TestLatestExtractWins:
    def test_the_newest_extract_in_the_month_is_read(self, tmp_path, run_date):
        run_date(date(2026, 9, 15))
        folder = tmp_path / "trigger_8" / "SEPTEMBER_2026"
        write_extract(folder, "trigger8_20260901_030000_000000.json", "early-draft")
        write_extract(folder, "trigger8_20260930_143022_123456.json", "final")
        write_extract(folder, "trigger8_20260915_090000_500000.json", "mid-draft")

        source = TriggerSource(
            SourceSettings(type="local", path=str(tmp_path / "trigger_8" / "{MONTH}_{YYYY}"))
        )
        assert tags_read(source) == ["final"]
        assert len(source.objects_read) == 1

    def test_last_months_folder_is_not_read(self, tmp_path, run_date):
        """The whole point of the per-month folder: a September run must not
        republish August's extract under fresh trigger IDs."""
        run_date(date(2026, 9, 15))
        write_extract(
            tmp_path / "trigger_8" / "AUGUST_2026",
            "trigger8_20260831_020000_000001.json",
            "august",
        )
        write_extract(
            tmp_path / "trigger_8" / "SEPTEMBER_2026",
            "trigger8_20260930_143022_123456.json",
            "september",
        )

        source = TriggerSource(
            SourceSettings(type="local", path=str(tmp_path / "trigger_8" / "{MONTH}_{YYYY}"))
        )
        assert tags_read(source) == ["september"]

    def test_ordering_is_by_parsed_timestamp_not_by_name(self, tmp_path, run_date):
        """This format happens to sort lexicographically; the code must not rely
        on that, or a day-first format would quietly select a month-old file."""
        run_date(date(2026, 9, 15))
        folder = tmp_path / "trigger_8" / "SEPTEMBER_2026"
        write_extract(folder, "trigger8_20260930_143022_123456.json", "final")
        write_extract(folder, "trigger8_20260901_030000_000000.json", "early")

        source = TriggerSource(
            SourceSettings(type="local", path=str(folder), selection="latest")
        )
        ranks = [
            source._rank(str(folder / n))
            for n in (
                "trigger8_20260901_030000_000000.json",
                "trigger8_20260930_143022_123456.json",
            )
        ]
        assert [r[0] for r in ranks] == [1, 1]  # both parsed
        assert ranks[0][1] < ranks[1][1]

    def test_a_file_with_no_timestamp_loses_to_one_that_has_it(self, tmp_path, run_date):
        run_date(date(2026, 9, 15))
        folder = tmp_path / "trigger_8" / "SEPTEMBER_2026"
        # Sorts after the real extract by name, so a name sort would pick it.
        write_extract(folder, "zz_manual_reupload.json", "manual")
        write_extract(folder, "trigger8_20260930_143022_123456.json", "final")

        source = TriggerSource(SourceSettings(type="local", path=str(folder)))
        assert tags_read(source) == ["final"]

    def test_selection_all_reads_every_extract(self, tmp_path, run_date):
        run_date(date(2026, 9, 15))
        folder = tmp_path / "trigger_8" / "SEPTEMBER_2026"
        write_extract(folder, "trigger8_20260901_030000_000000.json", "early")
        write_extract(folder, "trigger8_20260930_143022_123456.json", "final")

        source = TriggerSource(
            SourceSettings(type="local", path=str(folder), selection="all")
        )
        assert sorted(tags_read(source)) == ["early", "final"]

    def test_one_extract_needs_no_selection(self, tmp_path, run_date):
        run_date(date(2026, 9, 15))
        folder = tmp_path / "trigger_8" / "SEPTEMBER_2026"
        write_extract(folder, "trigger8_20260930_143022_123456.json", "final")

        source = TriggerSource(SourceSettings(type="local", path=str(folder)))
        assert tags_read(source) == ["final"]

    def test_preflight_probes_the_object_the_run_will_read(self, tmp_path, run_date):
        """first_object feeds preflight; it must not name a different file from
        the one the batch then reads."""
        run_date(date(2026, 9, 15))
        folder = tmp_path / "trigger_8" / "SEPTEMBER_2026"
        write_extract(folder, "trigger8_20260901_030000_000000.json", "early")
        selected = write_extract(folder, "trigger8_20260930_143022_123456.json", "final")

        source = TriggerSource(SourceSettings(type="local", path=str(folder)))
        assert source.first_object() == str(selected)

    def test_a_missing_month_folder_raises_locally(self, tmp_path, run_date):
        """Local and S3 differ here: a local path must exist, while a missing S3
        prefix simply lists empty. The S3 case is covered below, under moto."""
        from ifc_trigger_connector.utility.connector_utility import SourceAccessError

        run_date(date(2026, 11, 2))
        write_extract(
            tmp_path / "trigger_8" / "SEPTEMBER_2026",
            "trigger8_20260930_143022_123456.json",
            "september",
        )

        source = TriggerSource(
            SourceSettings(type="local", path=str(tmp_path / "trigger_8" / "{MONTH}_{YYYY}"))
        )
        with pytest.raises(SourceAccessError):
            list(source.stream())


# ---------------------------------------------------------------------------
# The deployed path: S3, under moto
# ---------------------------------------------------------------------------

moto = pytest.importorskip("moto", reason="moto is needed for the S3 selection tests")

import boto3  # noqa: E402

REGION = "eu-west-2"
TEMPLATE = "s3://location/trigger_8/{MONTH}_{YYYY}/"


@pytest.fixture
def bucket(monkeypatch):
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.setenv(name, "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_CA_BUNDLE", raising=False)

    with moto.mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(
            Bucket="location", CreateBucketConfiguration={"LocationConstraint": REGION}
        )

        def put(key, tag):
            s3.put_object(
                Bucket="location",
                Key=key,
                Body=json.dumps(
                    [
                        {
                            "triggerType": "IFC_CDD",
                            "triggerSubType": "TRIGGER_8",
                            "attributes": {
                                "counterparty_full_legal_entity_name": tag,
                                "counterparty_csid_sds": 9912345678,
                                "region": "EMEA",
                                "business_date": "2026-08-31",
                            },
                        }
                    ]
                ).encode(),
            )

        yield put


class TestAgainstS3:
    def test_the_run_month_selects_the_folder_and_the_newest_extract(self, bucket, run_date):
        bucket("trigger_8/AUGUST_2026/trigger8_20260831_020000_000001.json", "august")
        bucket("trigger_8/SEPTEMBER_2026/trigger8_20260901_030000_000000.json", "sept-draft")
        bucket("trigger_8/SEPTEMBER_2026/trigger8_20260930_143022_123456.json", "sept-final")
        bucket("trigger_8/OCTOBER_2026/trigger8_20261031_010000_000000.json", "october")

        run_date(date(2026, 9, 15))
        source = TriggerSource(SourceSettings(type="s3", path=TEMPLATE))

        assert tags_read(source) == ["sept-final"]
        assert source.objects_read == [
            "s3://location/trigger_8/SEPTEMBER_2026/trigger8_20260930_143022_123456.json"
        ]

    @pytest.mark.parametrize(
        "day, expected",
        [
            (date(2026, 8, 5), "august"),
            (date(2026, 9, 15), "sept-final"),
            (date(2026, 10, 1), "october"),
        ],
    )
    def test_each_run_month_reads_its_own_folder(self, bucket, run_date, day, expected):
        bucket("trigger_8/AUGUST_2026/trigger8_20260831_020000_000001.json", "august")
        bucket("trigger_8/SEPTEMBER_2026/trigger8_20260930_143022_123456.json", "sept-final")
        bucket("trigger_8/OCTOBER_2026/trigger8_20261031_010000_000000.json", "october")

        run_date(day)
        assert tags_read(TriggerSource(SourceSettings(type="s3", path=TEMPLATE))) == [expected]

    def test_a_month_folder_that_does_not_exist_yet_reads_nothing(self, bucket, run_date):
        """Unlike a local path, a missing S3 prefix lists empty - so the runner
        sees a zero-record batch and reports ZERO_RECORDS, not a read failure."""
        bucket("trigger_8/SEPTEMBER_2026/trigger8_20260930_143022_123456.json", "sept")

        run_date(date(2026, 11, 2))
        source = TriggerSource(SourceSettings(type="s3", path=TEMPLATE))
        assert list(source.stream()) == []
        assert source.objects_read == []
