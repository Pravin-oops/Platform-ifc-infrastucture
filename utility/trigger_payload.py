from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence


class DataType(str, Enum):
    STRING = "STRING"
    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
    DATE = "DATE"
    DATETIME = "DATETIME"
    BOOLEAN = "BOOLEAN"


DATE_FORMAT = "%y-%m-%d"


POLICY_NONE = ""
POLICY_NAME = "UK_TOK_AC_L0R0_UNC_DE"


class PayloadBuildError(ValueError):
    def __init__(self, message: str, *, field_name: str, source_key: str, check: str):
        super().__init__(message)
        self.field_name = field_name
        self.source_key = source_key
        self.check = check


@dataclass(frozen=True)
class FieldSpec:
    name: str
    source: str
    data_type: DataType = DataType.STRING
    required: bool = True
    encryption_policy: str = POLICY_NONE
    transform: Optional[Callable[[Any], Any]] = None
    default: Any = None
    max_length: Optional[int] = None


def normalise_identifier(value: Any) -> Any:
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _stringify(value: Any, data_type: DataType, spec: FieldSpec) -> str:
    try:
        if data_type is DataType.BOOLEAN:
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered not in {"true", "false", "y", "n", "yes", "no", "1", "0"}:
                    raise ValueError(f"not a boolean: {value!r}")
                return "true" if lowered in {"true", "y", "yes", "1"} else "false"
            return "true" if bool(value) else "false"

        if data_type is DataType.INTEGER:
            if isinstance(value, float) and not value.is_integer():
                raise ValueError(f"not an integer: {value!r}")
            return str(int(value))

        if data_type is DataType.DECIMAL:
            return f"{Decimal(str(value)):.2f}"

        if data_type is DataType.DATE:
            if isinstance(value, datetime):
                value = value.date()
            elif not isinstance(value, date):
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
            check=f"not a valid {data_type.value}",
        ) from exc


def build_fields(
    specs: Sequence[FieldSpec],
    attributes: Dict[str, Any],
    *,
    declare_policies: bool = True,
) -> List[Dict[str, str]]:
    fields: List[Dict[str, str]] = []

    for spec in specs:
        value = attributes.get(spec.source)
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
                    check="missing",
                )
            continue

        rendered = _stringify(value, spec.data_type, spec)

        if spec.max_length is not None and len(rendered) > spec.max_length:
            raise PayloadBuildError(
                f"Field '{spec.name}' is {len(rendered)} characters; the consumer "
                f"accepts at most {spec.max_length}",
                field_name=spec.name,
                source_key=spec.source,
                check=f"longer than {spec.max_length} characters",
            )

        if not rendered:
            if spec.required:
                raise PayloadBuildError(
                    f"Required payload field '{spec.name}' rendered empty",
                    field_name=spec.name,
                    source_key=spec.source,
                    check="empty",
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
            check="no fields",
        )

    return fields


def serialise(fields: List[Dict[str, str]]) -> str:
    return json.dumps(fields, separators=(",", ":"), ensure_ascii=False)
