"""The BSP trigger payload contract.

BSP does not accept a domain object in the envelope's ``payload`` field. It
accepts a JSON *string* holding a list of self-describing fields::

    [{"fieldName": ..., "fieldValue": ...,
      "fieldEncryptionPolicy": ..., "fieldDataType": ...}, ...]

The string is the array itself. The envelope field is already called
``payload``, so wrapping the array in a ``{"payload": ...}`` object would nest a
payload inside the payload, which the Trigger Backbone consumer rejects.

Every value travels as a string; ``fieldDataType`` tells the consumer how to
read it. That is what lets one topic serve three business units without a schema
change per trigger, and it is why each trigger's field set is declared as data
(``FieldSpec`` lists in ``trigger_definitions``) rather than hand-built dicts.

Two contract details bite if missed:

* ``fieldValue`` has ``minLength: 1``. An optional field with no value must be
  *omitted*, not sent empty - "" fails registry-side validation.
* ``fieldEncryptionPolicy`` is mandatory but may be empty. Only PII fields carry
  a tokenisation policy.

``build_fields`` below is the only thing that constructs payloads, and it cannot
produce a document that breaks either rule.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence


class DataType(str, Enum):
    """Values permitted in ``fieldDataType``, upper case as the Trigger Backbone expects."""

    STRING = "STRING"
    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
    DATE = "DATE"
    DATETIME = "DATETIME"
    BOOLEAN = "BOOLEAN"


# Tokenisation policies. NAME applies only to the Relationship Owner Name field.
#
# PII arrives already tokenised, so the connector never de-tokenises: it declares
# the policy applied upstream so the consumer knows how to read the value. Only
# PROD data is tokenised (envelope.tokenised_environments), so everywhere else
# build_fields is told not to declare policies and every field goes out with "".
POLICY_NONE = ""
POLICY_NAME = "UK_TOK_AC_L0R0_UNC_DE"


class PayloadBuildError(ValueError):
    """A required field was missing, empty or unconvertible."""

    def __init__(self, message: str, *, field_name: str, source_key: str):
        super().__init__(message)
        self.field_name = field_name
        self.source_key = source_key


@dataclass(frozen=True)
class FieldSpec:
    """One field in a trigger's payload contract.

    ``name`` is what BSP consumers see; ``source`` is the key the upstream
    produces. Keeping them separate means an upstream column rename is a one-line
    change here rather than a breaking change on the topic.
    """

    name: str
    source: str
    data_type: DataType = DataType.STRING
    required: bool = True
    encryption_policy: str = POLICY_NONE
    #: Applied before string conversion; used to flatten lists.
    transform: Optional[Callable[[Any], Any]] = None
    #: Used when the source key is absent. For optional fields and for fixed
    #: contract values such as Trigger_subType_detail.
    default: Any = None
    #: Maximum length of the *rendered* string, from the consumer's data-length
    #: column. A longer value is rejected rather than truncated: the consumer
    #: sizes its columns to these numbers, and a silently clipped CSID or BRID
    #: is a wrong identifier, not a cosmetic one.
    max_length: Optional[int] = None


def normalise_identifier(value: Any) -> Any:
    """Render an identifier that a reader widened to float as the integer it holds.

    A JSON reader that turns a bigint CSID into ``9912345678.0`` would otherwise
    publish that string - a different identifier, and one character over the
    consumer's width. Non-integral floats are left alone so they fail the field's
    own validation rather than being silently rounded.
    """
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _stringify(value: Any, data_type: DataType, spec: FieldSpec) -> str:
    """Render ``value`` as the string BSP expects for ``data_type``."""
    try:
        if data_type is DataType.BOOLEAN:
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered not in {"true", "false", "y", "n", "yes", "no", "1", "0"}:
                    raise ValueError(f"not a boolean: {value!r}")
                return "true" if lowered in {"true", "y", "yes", "1"} else "false"
            return "true" if bool(value) else "false"

        if data_type is DataType.INTEGER:
            # int(float) would silently truncate 10.7 to 10 on a count field.
            if isinstance(value, float) and not value.is_integer():
                raise ValueError(f"not an integer: {value!r}")
            return str(int(value))

        if data_type is DataType.DECIMAL:
            # Decimal(str(...)) avoids binary-float artefacts on money amounts.
            return f"{Decimal(str(value)):.2f}"

        if data_type is DataType.DATE:
            if isinstance(value, datetime):
                return value.date().isoformat()
            if isinstance(value, date):
                return value.isoformat()
            return str(value).strip()[:10]

        if data_type is DataType.DATETIME:
            if isinstance(value, datetime):
                return value.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            return str(value).strip()

        return str(value).strip()

    except (ValueError, TypeError, InvalidOperation) as exc:
        raise PayloadBuildError(
            f"Field '{spec.name}' value {value!r} is not a valid {data_type.value}: {exc}",
            field_name=spec.name,
            source_key=spec.source,
        ) from exc


def build_fields(
    specs: Sequence[FieldSpec],
    attributes: Dict[str, Any],
    *,
    declare_policies: bool = True,
) -> List[Dict[str, str]]:
    """Project ``attributes`` through ``specs`` into BSP payload field objects.

    ``declare_policies`` False sends every ``fieldEncryptionPolicy`` empty: the
    environment's data is not tokenised, so naming a policy would tell the
    consumer to de-tokenise a plain value.

    A missing or blank source value is replaced by ``spec.default`` when the
    spec declares one. Raises ``PayloadBuildError`` when a required field is
    still missing or empty after that, or when any field's rendered value is
    longer than its ``max_length``. Optional fields with no value are omitted,
    because the contract forbids an empty ``fieldValue``.
    """
    fields: List[Dict[str, str]] = []

    for spec in specs:
        value = attributes.get(spec.source)
        # A blank cell in the extract means the same thing as an absent column,
        # so both fall back to the default before the required check below.
        if value is None or (isinstance(value, str) and not value.strip()):
            value = spec.default

        if value is not None and spec.transform is not None:
            value = spec.transform(value)

        if value is None or (isinstance(value, str) and not value.strip()):
            if spec.required:
                raise PayloadBuildError(
                    f"Required payload field '{spec.name}' is missing "
                    f"(source attribute '{spec.source}')",
                    field_name=spec.name,
                    source_key=spec.source,
                )
            continue

        rendered = _stringify(value, spec.data_type, spec)

        if spec.max_length is not None and len(rendered) > spec.max_length:
            raise PayloadBuildError(
                f"Field '{spec.name}' is {len(rendered)} characters; the consumer "
                f"accepts at most {spec.max_length}",
                field_name=spec.name,
                source_key=spec.source,
            )

        if not rendered:
            if spec.required:
                raise PayloadBuildError(
                    f"Required payload field '{spec.name}' rendered empty",
                    field_name=spec.name,
                    source_key=spec.source,
                )
            continue

        fields.append(
            {
                "fieldName": spec.name,
                "fieldValue": rendered,
                "fieldEncryptionPolicy": spec.encryption_policy if declare_policies else POLICY_NONE,
                "fieldDataType": spec.data_type.value,
            }
        )

    if not fields:
        raise PayloadBuildError(
            "Payload would be empty; the BSP contract requires at least one field",
            field_name="<payload>",
            source_key="<none>",
        )

    return fields


def serialise(fields: List[Dict[str, str]]) -> str:
    """The JSON string that goes into the envelope's ``payload`` field.

    Compact separators and preserved key order: the envelope is size limited, and
    a stable rendering keeps the trigger ID hash stable.
    """
    return json.dumps(fields, separators=(",", ":"), ensure_ascii=False)
