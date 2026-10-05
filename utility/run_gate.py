"""Entry-point gate: should this invocation process, or exit?

EventBridge fires on three consecutive dates per trigger because it cannot tell
a weekday from a weekend. Three consecutive dates always contain at least one
weekday, so the container decides:

* weekend            -> exit;
* month already done -> exit;
* otherwise          -> process, and record the month once it has delivered.

Every invocation appends its outcome to the run marker file (see ``RunMarker``).
Only ``SUCCESS`` closes a month; a run that found nothing records ``FAILURE`` or
``NOT RAN``, so the next date in the window retries - which is the
Databricks-failure case. That also means there is nothing to roll back when a
run fails.

Both skips exit 0: six no-op invocations a month is the scheme working.
"""

from __future__ import annotations

import calendar
import json
import logging
import os
import re
from datetime import date, datetime, timezone
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

from utility.connector_utility import is_s3_path, parse_s3_path, s3

logger = logging.getLogger(__name__)

#: The weekday decision is a UK business question: a run starting 23:30 UTC on a
#: Sunday in summer is already Monday in London.
TIMEZONE = "Europe/London"


def today(tz: str = TIMEZONE) -> date:
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo(tz)).date()


_MONTH_NAMES = {calendar.month_name[n].upper(): n for n in range(1, 13)}

#: ``run.month`` (``IFC_RUN__MONTH``) for this process, as the first of that
#: month; ``None`` runs the current month. Set by ``load_settings``.
_execution_month: Optional[date] = None


def parse_month(value: Any) -> date:
    """``2026-08``, ``AUGUST_2026`` or ``August 2026`` as the first of that month.

    The second spelling is the recon folder's name, so an operator can paste
    the folder of the month they want reprocessed.
    """
    text = str(value).strip()
    match = re.fullmatch(r"(\d{4})-(\d{1,2})", text)
    if match:
        year, month = int(match.group(1)), int(match.group(2))
    else:
        match = re.fullmatch(r"([A-Za-z]+)[ _-](\d{4})", text)
        if not match or match.group(1).upper() not in _MONTH_NAMES:
            raise ValueError(f"run month must be YYYY-MM or MONTH_YYYY, got {value!r}")
        year, month = int(match.group(2)), _MONTH_NAMES[match.group(1).upper()]
    if not 1 <= month <= 12:
        raise ValueError(f"run month must be YYYY-MM or MONTH_YYYY, got {value!r}")
    return date(year, month, 1)


def set_execution_month(value: Any) -> None:
    """Pin the month this process runs as - a reprocess of a past month - or clear it."""
    global _execution_month
    _execution_month = parse_month(value) if value else None


def execution_date(tz: str = TIMEZONE) -> date:
    """The date the run's month is taken from.

    Today, unless ``run.month`` pins another month, when it is the first of
    that month. Everything keyed on the month - the Athena ``business_date``,
    the recon folder, the business month the records are stamped with, the
    marker's month - reads this. The weekend check does not: it is about the day the task runs.
    """
    return _execution_month or today(tz)


def is_weekend(day: date) -> bool:
    return day.weekday() >= 5


def month_of(day: date) -> str:
    """The execution month this run delivers: the current one."""
    return day.strftime("%Y-%m")


#: The month was delivered. The only status that stops later invocations.
STATUS_SUCCESS = "SUCCESS"
#: The connector ran and did not deliver the month.
STATUS_FAILURE = "FAILURE"
#: The connector did not run: a weekend, or upstream had nothing ready.
STATUS_NOT_RAN = "NOT RAN"


#: How many times an S3 append re-reads and retries after another writer
#: changed the file between our read and our write.
APPEND_ATTEMPTS = 5

#: S3's answers to a conditional PUT that lost the race.
_CONFLICT_CODES = frozenset({"PreconditionFailed", "ConditionalRequestConflict"})


class RunMarker:
    """An append-only JSON file of run outcomes: one line per invocation.

        {"trigger_id": "TRIGGER_8", "year_month": "2026-09", "run_date": "2026-09-03",
         "run_status": "SUCCESS", "records_processed": 412,
         "recorded_at": "2026-09-03T02:04:11.512Z"}

    The file is JSON Lines - each line one element of the history - rather than
    a single bracketed ``[...]`` array, because that is the layout Athena's JSON
    SerDe reads: one object per line, one row per object. A bracketed array
    would be read as one unparseable row. See ``deploy/athena-run-markers.sql``.

    ``path`` is an ``s3://bucket/key`` URI or a local file. S3 has no append, so
    an append is read, add a line, write back - with a conditional PUT (``If-Match``
    on the ETag read, ``If-None-Match: *`` for a new file) so two triggers
    finishing together cannot drop each other's line; the loser re-reads and
    retries.

    A month's outcome is its latest ``SUCCESS`` or ``FAILURE`` line. ``NOT RAN``
    lines never change it: they record that a date looked and did nothing -
    a weekend, upstream not ready, or a month already delivered - so the 4th's
    invocation after a delivered 3rd appends ``NOT RAN`` and the month stays
    ``SUCCESS``. Lines are only ever appended; none is rewritten.
    """

    def __init__(self, path: str, *, client: Any = None):
        self._path = path
        self._client = client

    @property
    def path(self) -> str:
        return self._path

    @property
    def _s3(self) -> Any:
        if self._client is None:
            self._client = s3()
        return self._client

    # -- raw file access ----------------------------------------------------

    def _read(self) -> Tuple[str, Optional[str]]:
        """The file's text and, on S3, its ETag. A missing file is empty."""
        if is_s3_path(self._path):
            bucket, key = parse_s3_path(self._path)
            try:
                obj = self._s3.get_object(Bucket=bucket, Key=key)
            except Exception as exc:
                if _error_code(exc) in ("NoSuchKey", "404"):
                    return "", None
                raise
            return obj["Body"].read().decode("utf-8"), obj.get("ETag")

        try:
            with open(self._path, "r", encoding="utf-8") as handle:
                return handle.read(), None
        except FileNotFoundError:
            return "", None

    def _append(self, line: str) -> None:
        if not is_s3_path(self._path):
            existing, _ = self._read()
            os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
            with open(self._path, "a", encoding="utf-8") as handle:
                handle.write(_joined("" if not existing else existing[-1:], line))
            return

        bucket, key = parse_s3_path(self._path)
        for attempt in range(1, APPEND_ATTEMPTS + 1):
            existing, etag = self._read()
            condition = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}
            try:
                self._s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=(existing + _joined(existing[-1:], line)).encode("utf-8"),
                    ContentType="application/json",
                    **condition,
                )
                return
            except Exception as exc:
                if _error_code(exc) not in _CONFLICT_CODES or attempt == APPEND_ATTEMPTS:
                    raise
                logger.info(
                    "Run marker file changed while appending; re-reading",
                    extra={"run_marker_path": self._path, "attempt": attempt},
                )

    # -- the history --------------------------------------------------------

    def history(self, trigger: Optional[str] = None, month: Optional[str] = None) -> List[Dict[str, Any]]:
        """Every recorded outcome, oldest first, optionally for one trigger/month.

        A line that is not a JSON object is an error rather than skipped: the
        skipped line might be the SUCCESS, and skipping it would republish the
        month.
        """
        text, _ = self._read()
        records: List[Dict[str, Any]] = []
        for number, raw in enumerate(text.splitlines(), start=1):
            if not raw.strip():
                continue
            try:
                record = json.loads(raw)
            except ValueError as exc:
                raise ValueError(f"Run marker file {self._path} line {number} is not valid JSON: {exc}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"Run marker file {self._path} line {number} is not a JSON object")
            if trigger is not None and record.get("trigger_id") != trigger:
                continue
            if month is not None and record.get("year_month") != month:
                continue
            records.append(record)
        return records

    def latest(self, trigger: str, month: str) -> Optional[Dict[str, Any]]:
        """The most recent line for this trigger and month, or ``None``."""
        found = self.history(trigger, month)
        return found[-1] if found else None

    def status(self, trigger: str, month: str) -> Optional[str]:
        """This month's outcome, or ``None`` if never attempted.

        The latest line that is not ``NOT RAN``; ``NOT RAN`` only when nothing
        else has happened yet. So a skipped invocation after a delivery leaves
        the month ``SUCCESS``, while a forced re-delivery that failed after a
        SUCCESS reopens it.
        """
        found = self.history(trigger, month)
        ran = [r for r in found if r.get("run_status") != STATUS_NOT_RAN]
        record = (ran or found or [None])[-1]
        return record.get("run_status") if record else None

    def is_done(self, trigger: str, month: str) -> bool:
        """Whether this month is finished - delivered, not merely attempted.

        A FAILURE outcome, or only NOT RAN lines, means the month still has to
        be retried, so the presence of a line is not the question; the outcome is.
        """
        return self.status(trigger, month) == STATUS_SUCCESS

    def record(
        self,
        trigger: str,
        month: str,
        *,
        status: str,
        records: int = 0,
        day: Optional[date] = None,
        reason: str = "",
    ) -> None:
        """Append this invocation's outcome.

        ``records`` is what the run actually delivered, so it is zero for
        anything but a ``SUCCESS``. ``reason`` says why - it is what tells a
        weekend ``NOT RAN`` from an already-delivered one in Athena.
        """
        line = json.dumps(
            {
                "trigger_id": trigger,
                "year_month": month,
                "run_date": (day or today()).isoformat(),
                "run_status": status,
                "records_processed": int(records),
                "reason": reason,
                "recorded_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            },
            separators=(",", ":"),
        )
        self._append(line)

    def mark_done(self, trigger: str, month: str, *, records: int, day: Optional[date] = None,
                  reason: str = "") -> None:
        self.record(trigger, month, status=STATUS_SUCCESS, records=records, day=day, reason=reason)

    def mark_failed(self, trigger: str, month: str, *, records: int = 0, day: Optional[date] = None,
                    reason: str = "") -> None:
        self.record(trigger, month, status=STATUS_FAILURE, records=records, day=day, reason=reason)

    def mark_not_ran(self, trigger: str, month: str, *, day: Optional[date] = None,
                     reason: str = "") -> None:
        self.record(trigger, month, status=STATUS_NOT_RAN, records=0, day=day, reason=reason)


def _joined(last_char: str, line: str) -> str:
    """``line`` as the file's next line, whether or not the file ends in one.

    A hand-edited file that lost its final newline must not glue the new record
    onto the last one, which would turn two valid lines into one invalid one.
    """
    return ("" if last_char in ("", "\n") else "\n") + line + "\n"


def _error_code(exc: BaseException) -> Optional[str]:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return response.get("Error", {}).get("Code")
    return None


class GateOutcome(NamedTuple):
    """What the gate decided, and what the caller owes the marker file.

    ``mark_not_ran`` asks the caller to append a ``NOT RAN`` line: every skip
    - a weekend, or a month already delivered - records that this date looked
    and did nothing. A forced or due run owes nothing yet; it records its own
    outcome when it finishes.
    """

    proceed: bool
    reason: str
    mark_not_ran: bool = False


def should_run(
    trigger: str,
    marker: RunMarker,
    *,
    day: Optional[date] = None,
    month: Optional[str] = None,
    force: bool = False,
) -> GateOutcome:
    """Decide whether to process. ``day`` is injectable so this is testable.

    ``month`` is the month being delivered, ``YYYY-MM``; it defaults to
    ``day``'s, and differs only when ``run.month`` reprocesses a past month.
    """
    day = day or today()
    month = month or month_of(day)

    # Force first: it is the operator's deliberate re-delivery and overrides
    # both of the checks below, including a month already marked SUCCESS.
    if force:
        return GateOutcome(True, f"forced run for {trigger} {month}", False)

    # Then the delivered check, ahead of the weekend, so the reason recorded is
    # the one that matters. Its NOT RAN line leaves the SUCCESS in place: NOT RAN
    # never changes a month's outcome.
    if marker.is_done(trigger, month):
        return GateOutcome(False, f"{trigger} {month} was already delivered", True)

    if is_weekend(day):
        return GateOutcome(False, f"{day} is a {day.strftime('%A')}", True)

    return GateOutcome(True, f"{trigger} {month} is due", False)
