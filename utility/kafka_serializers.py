from __future__ import annotations

import io
import logging
import struct
from typing import Any, Dict

from fastavro import parse_schema, schemaless_writer

from utility import failure_catalog as catalog
from utility.error_classifier import RecordRejected

logger = logging.getLogger(__name__)

MAGIC_BYTE = 0
_HEADER = struct.Struct(">bI")


class AvroSerializer:
    def __init__(self, schema: Dict[str, Any], schema_id: int, *, name: str = "value"):
        if schema_id is None:
            raise ValueError("A schema id is required: every record is written in Confluent wire format")
        self._schema = schema
        self._parsed = parse_schema(schema)
        self._schema_id = schema_id
        self._name = name

    @property
    def schema(self) -> Dict[str, Any]:
        return self._schema

    @property
    def schema_id(self) -> int:
        return self._schema_id

    @property
    def header(self) -> bytes:
        return _HEADER.pack(MAGIC_BYTE, self._schema_id)

    def __call__(self, record: Dict[str, Any]) -> bytes:
        buffer = io.BytesIO()
        buffer.write(self.header)

        try:
            schemaless_writer(buffer, self._parsed, record)
        except Exception as exc:
            raise RecordRejected(
                f"Avro serialisation failed for the {self._name} schema: {exc}",
                catalog.SCHEMA_VALIDATION_FAILURE,
                detail={"triggerID": record.get("triggerID"), "error": str(exc), "check": "Avro serialisation"},
            ) from exc

        return buffer.getvalue()


class SizeGuard:
    def __init__(self, max_bytes: int):
        self._max_bytes = max_bytes

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    def check(self, payload: bytes, *, trigger_id: str, key: str, record: Dict[str, Any]) -> None:
        total = len(payload) + len(key.encode("utf-8"))
        if total <= self._max_bytes:
            return

        embedded = record.get("payload") or ""
        raise RecordRejected(
            f"Serialised message is {total} bytes, over the {self._max_bytes} byte limit",
            catalog.MESSAGE_TOO_LARGE,
            detail={
                "triggerID": trigger_id,
                "check": "message too large",
                "serialised_bytes": total,
                "limit_bytes": self._max_bytes,
                "payload_bytes": len(str(embedded).encode("utf-8")),
                "envelope_overhead_bytes": total - len(str(embedded).encode("utf-8")),
            },
        )


def build_serializer(
    *,
    schema: Dict[str, Any],
    schema_id: int,
    name: str = "value",
) -> AvroSerializer:
    serializer = AvroSerializer(schema, schema_id, name=name)
    logger.debug(
        "Avro serializer ready",
        extra={"schema_name": schema.get("name"), "schema_id": schema_id},
    )
    return serializer
