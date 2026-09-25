"""Streaming reader for the trigger event source.

Objects are read one at a time and yielded as they are parsed. Nothing
accumulates: the BUK Lambda's ``load_json_folder`` returns a list of every record
in the prefix, which is fine for a small batch and is precisely how a resident
container gets OOM-killed on a monthly run.

Three layouts are accepted, because upstream writers differ: a JSON object (one
event), a JSON array (many), or JSON Lines - which is what a Spark/Databricks
write produces and the only layout that streams without buffering the file.

``source.path`` may be a prefix or a single object. The ECS contract is the
latter: one ``.json`` file holding the whole batch as an array. That is read in
one piece - ``read_text`` decodes the whole object and ``json.loads`` builds the
whole list - so the task's memory has to cover roughly twice the file size.
Events are yielded one at a time from there, and the decoded text is released as
soon as the array is parsed. If the file ever outgrows the task, ``.jsonl`` from
upstream is the fix: it parses a line at a time and loses one record to a
malformed byte instead of the entire batch.

A malformed object yields a ``ParseFailure`` rather than raising, so one bad file
cannot abort a run that could have published everything else.

This is the only module that knows where data comes from. Swapping S3 for a
local folder needs no change here (paths are read as S3 when they start with
``s3://``); swapping in Athena means a second class with the same ``stream``.
"""

from __future__ import annotations

import json
import logging
import posixpath
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator, List, Optional, Set, Tuple, Union

from utility import failure_catalog as catalog
from utility.error_classifier import RecordRejected
from utility.connector_utility import (
    SourceAccessError,
    iter_object_paths,
    read_text,
)
from utility.connector_config import SourceSettings
from utility.tb_outcome_schema import TriggerEvent

logger = logging.getLogger(__name__)


def parse_filename_timestamp(name: str, *, pattern: str, fmt: str) -> Optional[datetime]:
    """Pull the extract timestamp out of a filename, or ``None``.

    ``None`` is not an error: a hand-placed file, or one TED names differently,
    simply cannot be ordered against the others and loses to any file that can.
    """
    match = re.search(pattern, name)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), fmt)
    except ValueError:
        # The pattern matched but the value is not a real date - a 13th month,
        # or a day-first string read as month-first. Not fatal: it just cannot
        # order this file.
        return None


@dataclass
class ParseFailure:
    """A record that could not even be parsed into a ``TriggerEvent``."""

    source_object: str
    index: int
    error: str
    raw: Optional[str] = None
    scenario_key: str = catalog.SCHEMA_VALIDATION_FAILURE.key


SourceItem = Union[TriggerEvent, ParseFailure]


class TriggerSource:
    def __init__(self, settings: SourceSettings):
        self._settings = settings
        self._objects_read: List[str] = []

    @property
    def objects_read(self) -> List[str]:
        return list(self._objects_read)

    def _rank(self, path: str) -> Tuple[int, datetime, str]:
        """Sort key: a file whose timestamp parses beats one whose does not.

        The name is the final tie-break, so the choice is deterministic when two
        extracts carry the same timestamp.
        """
        stamp = parse_filename_timestamp(
            posixpath.basename(path.rstrip("/")),
            pattern=self._settings.filename_timestamp_pattern,
            fmt=self._settings.filename_timestamp_format,
        )
        if stamp is None:
            return (0, datetime.min, path)
        return (1, stamp, path)

    def selected_objects(self) -> List[str]:
        """The objects this run reads, newest last.

        Under ``selection: latest`` - the default - that is the single newest
        extract in the month's folder. TED rewrites the whole month rather than
        appending, so every older file beside it is a superseded draft and
        reading them all would republish stale content under fresh trigger IDs.
        """
        found = list(
            iter_object_paths(self._settings.resolved_path, self._settings.file_suffixes)
        )
        if not found or self._settings.selection == "all":
            return found

        latest = max(found, key=self._rank)
        undated = [p for p in found if self._rank(p)[0] == 0]

        if self._rank(latest)[0] == 0:
            # Nothing in the folder carries a parseable timestamp, so there is no
            # ordering to trust - say so rather than implying a real selection.
            logger.warning(
                "No source object has a parseable timestamp; selecting by name. "
                "Check source.filename_timestamp_pattern against what TED writes",
                extra={
                    "selected": latest,
                    "candidates": len(found),
                    "pattern": self._settings.filename_timestamp_pattern,
                },
            )
        elif undated:
            logger.warning(
                "Some source objects have no parseable timestamp and were not "
                "considered for selection",
                extra={"selected": latest, "undated": undated[:10], "undated_count": len(undated)},
            )

        if len(found) > 1:
            logger.info(
                "Selected the newest source object",
                extra={"selected": latest, "superseded": len(found) - 1},
            )
        return [latest]

    def first_object(self) -> Optional[str]:
        """Cheap readability probe for preflight; does not consume the stream.

        Reports the object the run will actually read, so preflight and the run
        cannot disagree about which file is the batch.
        """
        selected = self.selected_objects()
        return selected[0] if selected else None

    def _parse_object(self, path: str) -> Iterator[SourceItem]:
        try:
            body = read_text(path)
        except SourceAccessError as exc:
            # A single unreadable object is reported and skipped; a prefix-wide
            # permission problem will have failed preflight already.
            logger.error("Could not read source object", extra={"source_object": path, "error": str(exc)})
            yield ParseFailure(path, 0, str(exc), scenario_key=catalog.BDP_READ_FAILURE.key)
            return

        if path.lower().endswith(".jsonl"):
            for index, line in enumerate(body.splitlines()):
                line = line.strip()
                if line:
                    yield from self._to_event(line, path, index, raw_is_text=True)
            return

        try:
            document = json.loads(body)
        except ValueError as exc:
            yield ParseFailure(path, 0, f"Object is not valid JSON: {exc}", raw=body[:2000])
            return

        if isinstance(document, list):
            # The whole batch arrives as one array, so the decoded string and the
            # parsed list are both held at once - the peak of the run. Drop the
            # string before yielding: events go out one at a time from here, and
            # nothing needs the raw text again.
            logger.info(
                "Source object parsed",
                extra={"source_object": path, "records": len(document), "bytes": len(body)},
            )
            del body
            for index, item in enumerate(document):
                yield from self._to_event(item, path, index)
            return

        yield from self._to_event(document, path, 0)

    def _to_event(
        self, item: Any, path: str, index: int, *, raw_is_text: bool = False
    ) -> Iterator[SourceItem]:
        if raw_is_text:
            try:
                item = json.loads(item)
            except ValueError as exc:
                yield ParseFailure(path, index, f"Line is not valid JSON: {exc}", raw=str(item)[:2000])
                return

        try:
            yield TriggerEvent.from_dict(item, source_object=path, index=index)
        except RecordRejected as exc:
            yield ParseFailure(
                path,
                index,
                str(exc),
                raw=json.dumps(item, default=str)[:2000] if isinstance(item, (dict, list)) else str(item)[:2000],
            )

    def stream(self, *, skip_objects: Optional[Set[str]] = None, limit: Optional[int] = None) -> Iterator[SourceItem]:
        """Yield events across the prefix, oldest key first.

        ``skip_objects`` comes from the checkpoint of an interrupted run, so a
        restarted task does not re-read objects it already drained.
        """
        skip = skip_objects or set()
        emitted = 0
        max_records = limit or self._settings.max_records_per_batch

        for path in self.selected_objects():
            if path in skip:
                logger.debug("Skipping already-processed object", extra={"source_object": path})
                continue

            logger.info("Reading source object", extra={"source_object": path})
            self._objects_read.append(path)

            for item in self._parse_object(path):
                yield item
                emitted += 1
                # No cap by default: the ECS contract is one object holding the
                # whole batch, and stopping part way through it would drop the
                # rest with nothing left to come back to.
                if max_records is not None and emitted >= max_records:
                    logger.warning(
                        "Record limit reached; the rest of this run's source is NOT published. "
                        "Anything left inside the object just read is only picked up by a "
                        "re-run, which republishes them for the consumer to resolve",
                        extra={"limit": max_records, "last_object": path},
                    )
                    return
