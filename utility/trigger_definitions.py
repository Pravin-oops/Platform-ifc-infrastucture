"""IFC CDD trigger payload definitions for Triggers 8, 9 and 21."""

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

# triggerSubType values; the BSP schema's enum accepts only these spellings.
SUBTYPE_NEW_HRC_RELATIONSHIP = "NewHRCRelationship"
SUBTYPE_ACCOUNT_INACTIVITY = "AccountInactivity"
SUBTYPE_MULTIPLE_TM_SARS = "MultipleTMSARs"

#: Filled in when the source row has no value for these mandatory fields.
DEFAULT_BUSINESS_UNIT = "UK Corporate"
DEFAULT_LOCATION = "UK"

#: Consumer column widths from the payload specification; longer values are rejected.
MAX_LENGTHS = {
    "date_of_request": 10,                       # rendered YY-MM-DD (8)
    "counterparty_full_legal_entity_name": 100,
    "counterparty_csid_sds": 11,
    "client_relationship_owner_name": 50,
    "client_relationship_owner_brid": 10,
    "client_relationship_owner_business_unit": 20,
    "client_relationship_owner_location": 5,
    "region": 50,
}

#: Enum symbols this connector does not produce.
OUT_OF_SCOPE_SYMBOLS = {"UBOChanges", "SigChanges"}


@dataclass(frozen=True)
class TriggerDefinition:
    sub_type: str
    #: The triggerSubType enum symbol published for ``sub_type``.
    published_sub_type: str
    subtype_detail: str
    fields: Sequence[FieldSpec]
    #: Attribute that separates a customer's sub-events within one run.
    event_key_source: Optional[str] = None

    def field_names(self) -> List[str]:
        return [spec.name for spec in self.fields]

    def required_sources(self) -> List[str]:
        """Attribute keys an event must carry. Used to check a new data source."""
        return [spec.source for spec in self.fields if spec.required and spec.default is None]


def _payload_fields() -> List[FieldSpec]:
    """The eight fields every IFC trigger publishes, in contract order."""
    return [
        # The date the trigger file was generated. Source values arrive as a full
        # timestamp; DataType.DATE renders the date part only, as YY-MM-DD.
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


TRIGGER_8_DEFINITION = TriggerDefinition(
    sub_type=TRIGGER_8,
    published_sub_type=SUBTYPE_NEW_HRC_RELATIONSHIP,
    subtype_detail="Trig_8_Cross_Border_Transactions",
    # One row per counterparty per business date; business_date keys sub-events, unpublished.
    event_key_source="business_date",
    fields=_payload_fields(),
)


TRIGGER_9_DEFINITION = TriggerDefinition(
    sub_type=TRIGGER_9,
    published_sub_type=SUBTYPE_ACCOUNT_INACTIVITY,
    subtype_detail="Trig_9_Account_Inactivity",
    event_key_source="business_date",
    fields=_payload_fields(),
)


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

# Upstream spellings and the published symbols, mapped to the canonical sub-type.
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