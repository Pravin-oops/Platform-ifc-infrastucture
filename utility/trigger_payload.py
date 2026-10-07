"""The BSP trigger payload contract."""

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


#: How a DATE field's value is written: YY-MM-DD, as the payload specification
#: gives for Date of Request (2026-06-10 goes out as 26-06-10).
DATE_FORMAT = "%y-%m-%d"


# Tokenisation policies. PII arrives tokenised; NAME only declares the upstream policy.
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
    """One field in a trigger's payload contract."""

    name: str
    source: str
    data_type: DataType = DataType.STRING
    required: bool = True
    encryption_policy: str = POLICY_NONE
    #: Applied before string conversion; used to flatten lists.
    transform: Optional[Callable[[Any], Any]] = None
    #: Used when the source value is absent or blank.
    default: Any = None
    #: Maximum length of the rendered value; longer values are rejected, not truncated.
    max_length: Optional[int] = None


def normalise_identifier(value: Any) -> Any:
    """Render an identifier that a reader widened to float as the integer it holds."""
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
                value = value.date()
            elif not isinstance(value, date):
                # An ISO date or timestamp string: its date part is read as a
                # date, so anything that is not one fails rather than shipping.
                value = date.fromisoformat(str(value).strip()[:10])
            return value.strftime(DATE_FORMAT)

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
    """Project ``attributes`` through ``specs`` into BSP payload field objects."""
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
    """The JSON string that goes into the envelope's ``payload`` field."""
    return json.dumps(fields, separators=(",", ":"), ensure_ascii=False)
