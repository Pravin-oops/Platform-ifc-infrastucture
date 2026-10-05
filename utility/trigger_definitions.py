"""IFC CDD trigger payload definitions for Triggers 8, 9 and 21.

Each definition is the data contract between FRED (which writes the trigger
event into the Trigger BDP) and the IFC Backbone consumers. Changing a
``FieldSpec.name`` here is a consumer-visible change and needs the BSP schema
change process; changing a ``FieldSpec.source`` is an internal remap and does not.

The payload is now a *single* eight-field set, identical for every trigger:

    Date of Request, Counterparty Full Legal Entity Name, Counterparty ID,
    Client Relationship Owner Name, Client Relationship Owner BRID,
    Client Relationship Owner Business Unit,
    Client Relationship Owner Location, Region

Everything else the earlier drafts published - ``Trigger_subType_detail``,
``Customer Segment``, ``Business Date``, ``Last Run Date`` and the whole
Trigger 21 alert-volume block - is no longer on the wire. The trigger a message
belongs to is already carried by the envelope's ``triggerSubType``, so the
payload does not repeat it.

Only ``Client Relationship Owner Name`` carries a tokenisation policy
(``DPASS_POLICY_NAME``); every other field goes out with an empty policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from utility.trigger_payload import (
    POLICY_NAME,
    DataType,
    FieldSpec,
    normalise_identifier,
)

# Canonical internal keys. These are what FRED writes into the Trigger BDP and
# what the connector resolves on; they are *not* what goes on the wire.
TRIGGER_8 = "TRIGGER_8"
TRIGGER_9 = "TRIGGER_9"
TRIGGER_21 = "TRIGGER_21"

# Published values for the envelope's triggerSubType field. Since the BSP
# schema made triggerSubType an Avro *enum*, only these exact spellings can be
# serialised - anything else is an unencodable record, not a soft mismatch.
SUBTYPE_NEW_HRC_RELATIONSHIP = "NewHRCRelationship"
SUBTYPE_ACCOUNT_INACTIVITY = "AccountInactivity"
SUBTYPE_MULTIPLE_TM_SARS = "MultipleTMSARs"

#: Contract defaults for the two payload fields the specification defaults
#: rather than requires on the row. Both are mandatory on the wire; when the
#: source row omits one, the connector fills it in.
DEFAULT_BUSINESS_UNIT = "UK Corporate"
DEFAULT_LOCATION = "UK"

#: Consumer-side column widths, from the SIEBEL data-length column of the
#: payload specification. They size the consumer's table, so a value longer
#: than its width is rejected here rather than truncated on the wire: a clipped
#: CSID or BRID is a different identifier, not a cosmetic difference. Changing
#: one of these is a consumer-visible change, like changing a FieldSpec.name.
MAX_LENGTHS = {
    "date_of_request": 10,                       # rendered YYYY-MM-DD
    "counterparty_full_legal_entity_name": 100,
    "counterparty_csid_sds": 11,
    "client_relationship_owner_name": 50,
    "client_relationship_owner_brid": 10,
    "client_relationship_owner_business_unit": 20,
    "client_relationship_owner_location": 5,
    "region": 50,
}

#: Symbols the registered enum carries that this connector does not produce.
#: Held here so an event asking for one fails with "out of scope for this
#: connector" rather than "unknown trigger".
OUT_OF_SCOPE_SYMBOLS = {"UBOChanges", "SigChanges"}


@dataclass(frozen=True)
class TriggerDefinition:
    sub_type: str
    #: Value written to the envelope's triggerSubType enum. Separated from
    #: ``sub_type`` for the same reason FieldSpec separates name from source:
    #: the consumer-visible spelling is governed by the BSP schema change
    #: process, the internal key is ours to change freely.
    published_sub_type: str
    subtype_detail: str
    fields: Sequence[FieldSpec]
    #: Attribute that distinguishes sub-events for the same customer in one run.
    #: Trigger 8 raises one sub-event per high-risk country, Trigger 9 one per
    #: account. Without this in the sub-event key the second sub-event of a
    #: customer would be suppressed as a duplicate of the first.
    event_key_source: Optional[str] = None

    def field_names(self) -> List[str]:
        return [spec.name for spec in self.fields]

    def required_sources(self) -> List[str]:
        """Attribute keys an event must carry. Used to check a new data source."""
        return [spec.source for spec in self.fields if spec.required and spec.default is None]


def _payload_fields() -> List[FieldSpec]:
    """The eight fields every IFC trigger publishes, in contract order.

    One list, shared by all three definitions: the payload section is identical
    for Triggers 8, 9 and 21. A new trigger joins by reusing this list, not by
    declaring its own.

    All eight are mandatory: the requirement marks every attribute Mandatory = Y
    and demands that a row missing one is rejected rather than published with a
    hole. Two of them carry a contractual default - Business Unit and Location -
    so an absent source value is filled in rather than failing the row; every
    other field has to arrive on the row, and a row without it is quarantined
    with ``PayloadBuildError`` instead of going on the wire.

    Each field also carries the consumer's column width from ``MAX_LENGTHS``; a
    value longer than its width is quarantined the same way.
    """
    return [
        # The date the trigger file was generated. Source values arrive as a full
        # timestamp; DataType.DATE renders the date part only.
        FieldSpec(
            "Date of Request",
            "date_of_request",
            DataType.DATE,
            max_length=MAX_LENGTHS["date_of_request"],
        ),
        FieldSpec(
            "Counterparty Full Legal Entity Name",
            "counterparty_full_legal_entity_name",
            max_length=MAX_LENGTHS["counterparty_full_legal_entity_name"],
        ),
        # Held as a string: it is an identifier, never arithmetic, and a bigint
        # rendered as Integer would lose leading zeros if the source ever pads.
        FieldSpec(
            "Counterparty ID",
            "counterparty_csid_sds",
            transform=normalise_identifier,
            max_length=MAX_LENGTHS["counterparty_csid_sds"],
        ),
        FieldSpec(
            "Client Relationship Owner Name",
            "client_relationship_owner_name",
            encryption_policy=POLICY_NAME,
            max_length=MAX_LENGTHS["client_relationship_owner_name"],
        ),
        FieldSpec(
            "Client Relationship Owner BRID",
            "client_relationship_owner_brid",
            max_length=MAX_LENGTHS["client_relationship_owner_brid"],
        ),
        # Defaulted per the contract: the IFC UK-C feed is a single business
        # unit, so an absent value is the known constant rather than a gap.
        FieldSpec(
            "Client Relationship Owner Business Unit",
            "client_relationship_owner_business_unit",
            default=DEFAULT_BUSINESS_UNIT,
            max_length=MAX_LENGTHS["client_relationship_owner_business_unit"],
        ),
        FieldSpec(
            "Client Relationship Owner Location",
            "client_relationship_owner_location",
            default=DEFAULT_LOCATION,
            max_length=MAX_LENGTHS["client_relationship_owner_location"],
        ),
        FieldSpec("Region", "region", max_length=MAX_LENGTHS["region"]),
    ]


# ---------------------------------------------------------------------------
# Trigger 8 - bdp_corp_ifc_trigger_8
# ---------------------------------------------------------------------------

TRIGGER_8_DEFINITION = TriggerDefinition(
    sub_type=TRIGGER_8,
    published_sub_type=SUBTYPE_NEW_HRC_RELATIONSHIP,
    subtype_detail="Trig_8_Cross_Border_Transactions",
    # The table has no sub-event column; the grain is one row per counterparty
    # per business date, so the business date is what separates two rows for the
    # same counterparty inside one execution month. business_date is read from
    # the source row for the sub-event key only - it is not published.
    event_key_source="business_date",
    fields=_payload_fields(),
)


# ---------------------------------------------------------------------------
# Trigger 9 - bdp_corp_ifc_trigger_9
# ---------------------------------------------------------------------------

TRIGGER_9_DEFINITION = TriggerDefinition(
    sub_type=TRIGGER_9,
    published_sub_type=SUBTYPE_ACCOUNT_INACTIVITY,
    subtype_detail="Trig_9_Account_Inactivity",
    event_key_source="business_date",
    fields=_payload_fields(),
)


# ---------------------------------------------------------------------------
# Trigger 21 - bdp_corp_ifc_trigger_21 (multiple TM alerts / SARs)
#
# The table has the same columns as Triggers 8 and 9, and publishes the same
# eight header fields: there is no alert-volume column.
# ---------------------------------------------------------------------------

TRIGGER_21_DEFINITION = TriggerDefinition(
    sub_type=TRIGGER_21,
    published_sub_type=SUBTYPE_MULTIPLE_TM_SARS,
    subtype_detail="Trig_21_Multiple_TM_Alerts",
    event_key_source="business_date",
    fields=_payload_fields(),
)


DEFINITIONS: Dict[str, TriggerDefinition] = {
    TRIGGER_8: TRIGGER_8_DEFINITION,
    TRIGGER_9: TRIGGER_9_DEFINITION,
    TRIGGER_21: TRIGGER_21_DEFINITION,
}

# Tolerated spellings from upstream systems, mapped to the canonical sub-type.
# The published enum symbols are included so the connector still resolves an
# event once FRED starts writing the BSP spelling instead of the internal key.
_ALIASES = {
    "TRIGGER8": TRIGGER_8, "TRIG_8": TRIGGER_8, "T8": TRIGGER_8, "8": TRIGGER_8,
    "TRIGGER9": TRIGGER_9, "TRIG_9": TRIGGER_9, "T9": TRIGGER_9, "9": TRIGGER_9,
    "TRIGGER21": TRIGGER_21, "TRIG_21": TRIGGER_21, "T21": TRIGGER_21, "21": TRIGGER_21,
}
_ALIASES.update(
    {definition.published_sub_type.upper(): key for key, definition in DEFINITIONS.items()}
)

#: Every value published to the topic, for cross-checking against the enum
#: symbols in the bundled .avsc at startup.
PUBLISHED_SUB_TYPES = {
    definition.published_sub_type for definition in DEFINITIONS.values()
}


def resolve(sub_type: Any) -> TriggerDefinition:
    """Look up a definition, tolerating the common upstream spellings."""
    if sub_type is None:
        raise KeyError("triggerSubType is missing")

    key = str(sub_type).strip().upper().replace("-", "_").replace(" ", "_")
    if key in DEFINITIONS:
        return DEFINITIONS[key]
    if key in _ALIASES:
        return DEFINITIONS[_ALIASES[key]]

    if key in {symbol.upper() for symbol in OUT_OF_SCOPE_SYMBOLS}:
        raise KeyError(
            f"triggerSubType {sub_type!r} is a valid Trigger Backbone symbol but is not "
            f"produced by this connector (IFC CDD Triggers 8, 9 and 21 only)"
        )

    raise KeyError(
        f"Unsupported triggerSubType {sub_type!r}; expected one of "
        f"{sorted(DEFINITIONS)} or {sorted(PUBLISHED_SUB_TYPES)}"
    )