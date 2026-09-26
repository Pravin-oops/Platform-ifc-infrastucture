"""Build and validate the TriggerBackboneTopicSchema envelope.

Envelope values, as agreed with the Trigger Backbone:

* ``timestamp`` is the business month the triggers belong to, not the moment
  they were detected. Monthly triggers for June 2026 are produced in July, and
  every one of them carries the last instant of June,
  ``2026-06-30T23:59:59.999999999Z``. The business month is the calendar month
  before the run date, in UK time.
* ``triggerPostingTimestamp`` is when the event is posted, in the same RFC 3339
  shape: UTC, nanosecond precision, ``Z``.
* ``triggerType``, ``triggerOriginatingSystem``, ``triggerOriginatingBU``,
  ``idType`` and ``idSystem`` are fixed constants (see below), per the payload
  specification on Confluence. Neither the input event nor config can change them.
* ``idValue`` is the counterparty CSID from the BDP row
  (``counterparty_csid_sds``). Nothing upstream supplies a separate customer id,
  so the input wrapper carries no identity at all.

The single most important property here is that the same input event always
produces the same ``triggerID``. The BUK Lambda derives its trigger ID from
wall-clock time and an in-process counter, so a retried invocation republishes
the same business event under a new ID and the consumer sees a duplicate. On
ECS, where a task can be stopped and restarted mid-batch by a deployment or a
spot interruption, that would happen routinely.

So the ID is derived from the business key instead: system, trigger type and
sub-type, business month, CSID, and the sub-event discriminator the definition
nominates (the business date for Triggers 8 and 9). Re-running a month
republishes identical IDs, so the consuming team can recognise a republished
record as the one it already holds.
"""

from __future__ import annotations

import calendar
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Protocol

from fastavro import parse_schema
from fastavro.validation import validate as avro_validate

from utility import failure_catalog as catalog
from utility.error_classifier import PreflightError, RecordRejected
from utility import trigger_definitions as definitions
from utility.run_gate import today
from utility.trigger_definitions import TriggerDefinition
from utility.trigger_payload import (
    PayloadBuildError,
    build_fields,
    normalise_identifier,
    serialise,
)

logger = logging.getLogger(__name__)

# Envelope constants, per the payload specification on Confluence. An input
# event's own triggerType is ignored so upstream data cannot change them.
TRIGGER_TYPE = "KYCRefresh"
ORIGINATING_SYSTEM = "TBD"
ORIGINATING_BU = "UK-C and UK-ICB"
ID_TYPE = "Customer"
ID_SYSTEM = "Corelation id"

#: BDP column holding the counterparty CSID: the envelope's idValue and the
#: Kafka partition key.
CSID_SOURCE = "counterparty_csid_sds"

#: Avro int is signed 32-bit; sequence numbers must stay inside it.
MAX_SEQUENCE = 2**31 - 1

_BUSINESS_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def rfc3339(moment: datetime) -> str:
    """UTC RFC 3339 with nanosecond precision, e.g. ``2026-07-15T09:30:01.123456000Z``.

    Python datetimes stop at microseconds, so the last three digits are zero.
    """
    utc = moment.astimezone(timezone.utc)
    return f"{utc:%Y-%m-%dT%H:%M:%S}.{utc.microsecond:06d}000Z"


def now_timestamp() -> str:
    return rfc3339(datetime.now(timezone.utc))


def previous_month(run_date: date) -> str:
    """The business month a run on ``run_date`` delivers: the month before it."""
    if run_date.month == 1:
        return f"{run_date.year - 1:04d}-12"
    return f"{run_date.year:04d}-{run_date.month - 1:02d}"


def month_end_timestamp(business_month: str) -> str:
    """Last instant of ``YYYY-MM``, e.g. ``2026-06-30T23:59:59.999999999Z``."""
    if not _BUSINESS_MONTH.match(business_month or ""):
        raise ValueError(f"business month must be YYYY-MM, got {business_month!r}")
    year, month = int(business_month[:4]), int(business_month[5:])
    last_day = calendar.monthrange(year, month)[1]
    return f"{business_month}-{last_day:02d}T23:59:59.999999999Z"


def normalise_csid(value: Any) -> Optional[str]:
    """The CSID as a string, or None when it is absent or unusable.

    A reader that widens bigint to float would otherwise publish
    ``9912345678.0``; an integral float is rendered as the integer it holds.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, float):
        if not value.is_integer():
            return None
    value = normalise_identifier(value)
    text = str(value).strip()
    return text or None


class SequenceAllocator(Protocol):
    """Returns the next occurrence number for a customer.

    Every call is a distinct event, so every call advances that customer's
    count: the first event for a CSID is 1, a second is 2. Two events for one
    customer must never be handed the same number, because the number is part of
    their identity.
    """

    def allocate(self, csid: str) -> int: ...


@dataclass
class TriggerEvent:
    """One row of a trigger's Athena table.

    ``attributes`` is the row's columns verbatim. The event carries only what the
    row cannot say for itself: which trigger it belongs to, which is the run's.
    """

    trigger_sub_type: str
    attributes: Dict[str, Any]
    trigger_type: Optional[str] = None
    upstream_trigger_id: Optional[str] = None
    source_object: Optional[str] = None
    source_index: int = 0

    @property
    def csid(self) -> Optional[str]:
        return normalise_csid(self.attributes.get(CSID_SOURCE))


@dataclass
class BuiltRecord:
    """An envelope ready to serialise, plus everything the audit trail needs."""

    trigger_id: str
    record: Dict[str, Any]
    payload_fields: List[Dict[str, str]]
    definition: TriggerDefinition
    event: TriggerEvent
    business_key: str
    business_month: str

    @property
    def kafka_key(self) -> str:
        """Partition key.

        The CSID, not the trigger ID: all triggers for one counterparty must
        land on one partition so a consumer sees them in order. The envelope's
        sequenceNumber then orders related events within that partition.
        """
        return self.record["idValue"]


class EnvelopeBuilder:
    """Turns ``TriggerEvent`` objects into validated Avro-ready envelopes."""

    def __init__(
        self,
        *,
        avro_schema: Dict[str, Any],
        sequence_allocator: Optional[SequenceAllocator] = None,
        business_month: Optional[str] = None,
    ):
        self._schema = avro_schema
        self._parsed_schema = parse_schema(avro_schema)
        if sequence_allocator is None:
            # Imported here, not at module scope: the allocator needs
            # MAX_SEQUENCE from this module, and there must be exactly one
            # implementation - a second one drifts from this one the moment the
            # numbering rule changes, which is how the local dry run ended up
            # numbering by file position rather than by customer.
            from utility.sequence_allocator import (
                SequenceAllocator as _DefaultAllocator,
            )

            sequence_allocator = _DefaultAllocator()
        self._sequence = sequence_allocator
        # Fixed for the life of the builder, so every record of a run carries
        # the same business month even if the run crosses midnight.
        self._business_month = business_month or previous_month(today())
        self._event_timestamp = month_end_timestamp(self._business_month)
        self._assert_subtypes_encodable()

    @property
    def business_month(self) -> str:
        return self._business_month

    @property
    def event_timestamp(self) -> str:
        return self._event_timestamp

    def _assert_subtypes_encodable(self) -> None:
        """Fail at construction if a definition cannot be written to the wire.

        ``triggerSubType`` is an Avro enum. A symbol the schema does not carry
        is not a validation warning - every record of that trigger is
        unencodable. Catching it here turns a whole-batch quarantine into a
        startup abort with the missing symbol named.
        """
        spec = next(
            (f for f in self._schema.get("fields", []) if f.get("name") == "triggerSubType"),
            None,
        )
        type_ = spec.get("type") if spec else None
        if not isinstance(type_, dict) or type_.get("type") != "enum":
            return  # plain string schema: any spelling encodes

        symbols = set(type_.get("symbols", []))
        missing = sorted(definitions.PUBLISHED_SUB_TYPES - symbols)
        if missing:
            raise PreflightError(
                "triggerSubType enum in the bundled schema is missing symbols this connector "
                f"publishes: {missing}. Registered symbols: {sorted(symbols)}",
                catalog.SCHEMA_VALIDATION_FAILURE,
                context={"missing_symbols": missing, "schema_symbols": sorted(symbols)},
            )

    # -- identity ----------------------------------------------------------

    def business_key(
        self,
        event: TriggerEvent,
        definition: TriggerDefinition,
        occurrence: int = 1,
    ) -> str:
        """Canonical, stable identity of the business event.

        Rendered as sorted-key JSON so the digest cannot change because an
        upstream writer reordered its columns.

        ``occurrence`` is the customer's Nth event in this batch, and is only
        part of the key from the second onwards. Without it, two events for one
        customer sharing a sub-event discriminator hash to the same trigger ID -
        two different events published under one identity. Leaving the first
        occurrence out keeps the identity of every ordinary record exactly what
        it was before the field existed, so only a genuine repeat changes.
        """
        discriminator = (
            event.attributes.get(definition.event_key_source) if definition.event_key_source else None
        )

        parts = {
            "system": ORIGINATING_SYSTEM,
            "triggerType": TRIGGER_TYPE,
            # The published symbol, so the identity a consumer can reconstruct
            # from the message matches the one the connector hashed.
            "triggerSubType": definition.published_sub_type,
            "businessMonth": self._business_month,
            "idType": ID_TYPE,
            "idValue": event.csid or "",
            "eventKey": "" if discriminator is None else str(discriminator),
        }
        if occurrence > 1:
            parts["occurrence"] = str(occurrence)
        return json.dumps(parts, sort_keys=True, separators=(",", ":"))

    def trigger_id(self, business_key: str, definition: TriggerDefinition, event: TriggerEvent) -> str:
        digest = hashlib.sha256(business_key.encode("utf-8")).hexdigest()[:16]
        return (
            f"{ORIGINATING_SYSTEM}-"
            f"{TRIGGER_TYPE}-"
            f"{definition.published_sub_type}-"
            f"{self._business_month}-"
            f"{digest}"
        )

    # -- construction ------------------------------------------------------

    def build(self, event: TriggerEvent) -> BuiltRecord:
        """Build one envelope, or raise ``RecordRejected`` for quarantine."""
        # The run's trigger, already validated when the config was loaded.
        definition = definitions.resolve(event.trigger_sub_type)

        csid = event.csid
        if csid is None:
            raise RecordRejected(
                f"{definition.sub_type} record has no usable counterparty CSID "
                f"(attribute '{CSID_SOURCE}'), so idValue cannot be populated",
                catalog.SCHEMA_VALIDATION_FAILURE,
                detail={
                    "triggerSubType": definition.sub_type,
                    "field_name": "idValue",
                    "source_key": CSID_SOURCE,
                    "source_object": event.source_object,
                },
            )

        try:
            payload_fields = build_fields(definition.fields, event.attributes)
        except PayloadBuildError as exc:
            raise RecordRejected(
                f"{definition.sub_type} payload build failed: {exc}",
                catalog.SCHEMA_VALIDATION_FAILURE,
                detail={
                    "triggerSubType": definition.sub_type,
                    "field_name": exc.field_name,
                    "source_key": exc.source_key,
                    "source_object": event.source_object,
                },
            ) from exc

        # Allocated before the key is built: the occurrence is part of the
        # identity from the second event for a customer onwards, so it has to be
        # known before the trigger ID is hashed.
        sequence = self._sequence.allocate(csid)
        business_key = self.business_key(event, definition, occurrence=sequence)
        trigger_id = self.trigger_id(business_key, definition, event)

        record = {
            "triggerID": trigger_id,
            "triggerType": TRIGGER_TYPE,
            # The published enum symbol, not the internal key. triggerSubType is
            # an Avro enum on the BSP schema: an unknown spelling cannot be
            # encoded at all, so this must come from the definition.
            "triggerSubType": definition.published_sub_type,
            "timestamp": self._event_timestamp,
            "triggerPostingTimestamp": now_timestamp(),
            "sequenceNumber": int(sequence),
            "triggerOriginatingSystem": ORIGINATING_SYSTEM,
            "triggerOriginatingBU": ORIGINATING_BU,
            "idType": ID_TYPE,
            "idValue": csid,
            "idSystem": ID_SYSTEM,
            "upstreamTriggerID": event.upstream_trigger_id,
            "payload": serialise(payload_fields),
        }

        self.validate(record, definition)

        return BuiltRecord(
            trigger_id=trigger_id,
            record=record,
            payload_fields=payload_fields,
            definition=definition,
            event=event,
            business_key=business_key,
            business_month=self._business_month,
        )

    def validate(self, record: Dict[str, Any], definition: Optional[TriggerDefinition] = None) -> None:
        """Validate against the local .avsc before the record reaches Kafka.

        fastavro reports only that validation failed, so the offending fields are
        worked out here - a bare 'record is not an example of the schema' is
        useless at three in the morning.
        """
        try:
            if avro_validate(record, self._parsed_schema, raise_errors=False):
                return
        except Exception as exc:  # fastavro raises on structurally odd input
            raise RecordRejected(
                f"Avro validation error: {exc}",
                catalog.SCHEMA_VALIDATION_FAILURE,
                detail={"triggerID": record.get("triggerID")},
            ) from exc

        problems = []
        for spec in self._schema.get("fields", []):
            name = spec["name"]
            if name not in record:
                problems.append(f"{name}: missing")
                continue
            if not avro_validate({name: record[name]}, parse_schema(
                {"type": "record", "name": "F", "fields": [spec]}
            ), raise_errors=False):
                problems.append(f"{name}: {record[name]!r} does not match {spec['type']}")

        raise RecordRejected(
            "Envelope failed Avro validation: " + ("; ".join(problems) or "unknown field"),
            catalog.SCHEMA_VALIDATION_FAILURE,
            detail={
                "triggerID": record.get("triggerID"),
                "triggerSubType": definition.published_sub_type if definition else None,
                "problems": problems,
            },
        )


