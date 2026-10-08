"""Build and validate the TriggerBackboneTopicSchema envelope."""

from __future__ import annotations

import calendar
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
from utility.run_gate import execution_date
from utility.trigger_definitions import TriggerDefinition
from utility.trigger_payload import (
    PayloadBuildError,
    build_fields,
    normalise_identifier,
    serialise,
)

logger = logging.getLogger(__name__)

# Envelope constants; the input event's own triggerType is ignored.
TRIGGER_TYPE = "KYCRefresh"
ORIGINATING_BU = "UK-C"
ID_TYPE = "Customer"
ID_SYSTEM = "UK-C CRIME"

#: BDP column holding the counterparty CSID: the envelope's idValue.
CSID_SOURCE = "counterparty_csid_sds"

#: Avro int is signed 32-bit; sequence numbers must stay inside it.
MAX_SEQUENCE = 2**31 - 1

_BUSINESS_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def rfc3339(moment: datetime) -> str:
    """UTC RFC 3339 with nanosecond precision, e.g. ``2026-07-15T09:30:01.123456000Z``."""
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
    """The CSID as a string, or None when it is absent or unusable."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, float):
        if not value.is_integer():
            return None
    value = normalise_identifier(value)
    text = str(value).strip()
    return text or None


class SequenceAllocator(Protocol):
    """Numbers the batch and counts each customer's occurrences."""

    def allocate(self, csid: str) -> int: ...

    def occurrences(self, csid: str) -> int: ...


@dataclass
class TriggerEvent:
    """One row of a trigger's Athena table."""

    trigger_sub_type: str
    attributes: Dict[str, Any]
    trigger_type: Optional[str] = None
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
        """Message key: the trigger ID, as in the Trigger Backbone's reference records."""
        return self.trigger_id


class EnvelopeBuilder:
    """Turns ``TriggerEvent`` objects into validated Avro-ready envelopes."""

    def __init__(
        self,
        *,
        avro_schema: Dict[str, Any],
        originating_system: str,
        declare_encryption_policies: bool,
        sequence_allocator: Optional[SequenceAllocator] = None,
        business_month: Optional[str] = None,
    ):
        if not originating_system or not str(originating_system).strip():
            raise ValueError("EnvelopeBuilder needs the environment's originating system code")
        #: triggerOriginatingSystem, and the trigger ID's first part.
        self._system = str(originating_system).strip()
        #: Only where the upstream data is tokenised (PROD) do fields name their
        #: policy; required, so no caller can leave it to a default.
        self._declare_policies = bool(declare_encryption_policies)
        self._schema = avro_schema
        self._parsed_schema = parse_schema(avro_schema)
        if sequence_allocator is None:
            # Imported here to avoid a cycle: the allocator needs MAX_SEQUENCE from this module.
            from utility.sequence_allocator import (
                SequenceAllocator as _DefaultAllocator,
            )

            sequence_allocator = _DefaultAllocator()
        self._sequence = sequence_allocator
        # Fixed for the life of the builder, so every record of a run carries
        # the same business month even if the run crosses midnight.
        self._business_month = business_month or previous_month(execution_date())
        self._event_timestamp = month_end_timestamp(self._business_month)
        self._assert_subtypes_encodable()

    @property
    def business_month(self) -> str:
        return self._business_month

    @property
    def originating_system(self) -> str:
        return self._system

    @property
    def event_timestamp(self) -> str:
        return self._event_timestamp

    def _assert_subtypes_encodable(self) -> None:
        """Fail at construction if a definition cannot be written to the wire."""
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

    def business_key(
        self,
        event: TriggerEvent,
        definition: TriggerDefinition,
        occurrence: int = 1,
    ) -> str:
        """Canonical, stable identity of the business event."""
        discriminator = (
            event.attributes.get(definition.event_key_source) if definition.event_key_source else None
        )

        parts = {
            "system": self._system,
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

    def trigger_id(self, definition: TriggerDefinition, sequence: int) -> str:
        """``{system}_{triggerType}_{triggerSubType}_{timestamp}_{sequenceNumber}``."""
        return (
            f"{self._system}_"
            f"{TRIGGER_TYPE}_"
            f"{definition.published_sub_type}_"
            f"{self._event_timestamp}_"
            f"{sequence}"
        )

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
                    "check": "missing",
                    "source_object": event.source_object,
                },
            )

        try:
            payload_fields = build_fields(
                definition.fields, event.attributes, declare_policies=self._declare_policies
            )
        except PayloadBuildError as exc:
            raise RecordRejected(
                f"{definition.sub_type} payload build failed: {exc}",
                catalog.SCHEMA_VALIDATION_FAILURE,
                detail={
                    "triggerSubType": definition.sub_type,
                    "field_name": exc.field_name,
                    "source_key": exc.source_key,
                    "check": exc.check,
                    "source_object": event.source_object,
                },
            ) from exc

        # The batch position is the sequenceNumber and ends the trigger ID; the
        # customer's occurrence count separates repeats in the business key.
        sequence = self._sequence.allocate(csid)
        occurrence = self._sequence.occurrences(csid)
        business_key = self.business_key(event, definition, occurrence=occurrence)
        trigger_id = self.trigger_id(definition, sequence)

        record = {
            "triggerID": trigger_id,
            "triggerType": TRIGGER_TYPE,
            # triggerSubType is an Avro enum, so it must be the definition's published symbol.
            "triggerSubType": definition.published_sub_type,
            "timestamp": self._event_timestamp,
            "triggerPostingTimestamp": now_timestamp(),
            "sequenceNumber": int(sequence),
            "triggerOriginatingSystem": self._system,
            "triggerOriginatingBU": ORIGINATING_BU,
            "idType": ID_TYPE,
            "idValue": csid,
            "idSystem": ID_SYSTEM,
            # Nullable in the schema, and the trigger tables carry no upstream
            # trigger id: an IFC trigger is the origin, not a derived event.
            "upstreamTriggerID": None,
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
        """Validate against the local .avsc before the record reaches Kafka."""
        try:
            if avro_validate(record, self._parsed_schema, raise_errors=False):
                return
        except Exception as exc:  # fastavro raises on structurally odd input
            raise RecordRejected(
                f"Avro validation error: {exc}",
                catalog.SCHEMA_VALIDATION_FAILURE,
                detail={"triggerID": record.get("triggerID"), "check": "Avro validation"},
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
                "field_name": ", ".join(p.split(":", 1)[0] for p in problems) or None,
                "check": "Avro validation",
            },
        )


