"""Finding this month's recon document without republishing the config.

TED writes one recon folder per trigger per month - ``trigger8/SEPTEMBER_2026/``
- holding timestamped documents. The folder name comes from the run date, and
the newest document wins, ordered by the timestamp parsed out of its filename.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from utility.connector_config import expand_date_tokens
from utility.recon_gate import parse_filename_timestamp

PATTERN = r"(\d{8}_\d{6}_\d+)"
FORMAT = "%Y%m%d_%H%M%S_%f"


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


class TestFilenameTimestamp:
    def test_the_ted_filename_parses(self):
        stamp = parse_filename_timestamp(
            "BDP_Corp_Trigger_8_recon_20260930_143022_123456.json", pattern=PATTERN, fmt=FORMAT
        )
        assert stamp == datetime(2026, 9, 30, 14, 30, 22, 123456)

    def test_a_name_without_a_timestamp_is_none_not_an_error(self):
        assert parse_filename_timestamp("manual_upload.json", pattern=PATTERN, fmt=FORMAT) is None

    def test_a_matching_but_impossible_date_is_none(self):
        assert (
            parse_filename_timestamp(
                "BDP_Corp_Trigger_8_recon_20261331_143022_123456.json", pattern=PATTERN, fmt=FORMAT
            )
            is None
        )
