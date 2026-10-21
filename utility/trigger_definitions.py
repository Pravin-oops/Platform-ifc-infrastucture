from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from utility.trigger_payload import (
    POLICY_NAME,
    DataType,
    FieldSpec,
    normalise_identifier,
)

TRIGGER_8 = "TRIGGER_8"
TRIGGER_9 = "TRIGGER_9"
TRIGGER_21 = "TRIGGER_21"

SUBTYPE_NEW_HRC_RELATIONSHIP = "NewHRCRelationship"
SUBTYPE_ACCOUNT_INACTIVITY = "AccountInactivity"
SUBTYPE_MULTIPLE_TM_SARS = "MultipleTMSARs"

DEFAULT_BUSINESS_UNIT = "UK Corporate"
DEFAULT_LOCATION = "UK"

MAX_LENGTHS = {
    "date_of_request": 10,
    "counterparty_full_legal_entity_name": 100,
    "counterparty_csid_sds": 11,
    "client_relationship_owner_name": 50,
    "client_relationship_owner_brid": 10,
    "client_relationship_owner_business_unit": 20,
    "client_relationship_owner_location": 5,
    "region": 50,
}

OUT_OF_SCOPE_SYMBOLS = {"UBOChanges", "SigChanges"}


@dataclass(frozen=True)
class TriggerDefinition:
    sub_type: str
    published_sub_type: str
    subtype_detail: str
    fields: Sequence[FieldSpec]
    event_key_source: Optional[str] = None

    def field_names(self) -> List[str]:
        return [spec.name for spec in self.fields]

    def required_sources(self) -> List[str]:
        return [spec.source for spec in self.fields if spec.required and spec.default is None]


def _payload_fields() -> List[FieldSpec]:
    return [
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
        FieldSpec(
            "Counterparty SDS ID / CSID",
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
        FieldSpec("Client Region", "region", max_length=MAX_LENGTHS["region"]),
    ]


TRIGGER_8_DEFINITION = TriggerDefinition(
    sub_type=TRIGGER_8,
    published_sub_type=SUBTYPE_NEW_HRC_RELATIONSHIP,
    subtype_detail="Trig_8_Cross_Border_Transactions",
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

_ALIASES = {
    "TRIGGER8": TRIGGER_8, "TRIG_8": TRIGGER_8, "T8": TRIGGER_8, "8": TRIGGER_8,
    "TRIGGER9": TRIGGER_9, "TRIG_9": TRIGGER_9, "T9": TRIGGER_9, "9": TRIGGER_9,
    "TRIGGER21": TRIGGER_21, "TRIG_21": TRIGGER_21, "T21": TRIGGER_21, "21": TRIGGER_21,
}
_ALIASES.update(
    {definition.published_sub_type.upper(): key for key, definition in DEFINITIONS.items()}
)

PUBLISHED_SUB_TYPES = {
    definition.published_sub_type for definition in DEFINITIONS.values()
}


def resolve(sub_type: Any) -> TriggerDefinition:
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