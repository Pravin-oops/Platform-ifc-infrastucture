"""Every record is Confluent wire format, which is what KafkaAvroDeserializer reads.

A record without the ``0x00`` + schema id header starts straight at the Avro
body, and the consumer's Flink job fails on its first byte.
"""

from __future__ import annotations

import io
import struct

import pytest
from fastavro import parse_schema, schemaless_reader

from utility.connector_config import ConnectorSettings
from utility.error_classifier import PreflightError
from utility.kafka_factory import KafkaStackFactory, _NoTokenProvider
from utility.kafka_serializers import AvroSerializer
from utility.resilience_utility import ShutdownSignal

SCHEMA = {
    "type": "record",
    "name": "Trigger",
    "fields": [{"name": "triggerID", "type": "string"}],
}


def dev_settings(**registry) -> ConnectorSettings:
    return ConnectorSettings.model_validate(
        {
            "source": {"type": "local", "path": "/tmp/x"},
            "kafka": {"topic": "t", "bsp_config_path": "b.yaml"},
            "schema_registry": {"mode": "DEV", **registry},
            "state": {"backend": "memory"},
        }
    )


def resolve(settings: ConnectorSettings):
    factory = KafkaStackFactory(settings, metrics=None, shutdown=ShutdownSignal())
    return factory._resolve_schema(_NoTokenProvider())


class TestSerializer:
    def test_record_carries_magic_byte_and_schema_id(self):
        payload = AvroSerializer(SCHEMA, 4711)({"triggerID": "r" * 57})

        magic, schema_id = struct.unpack(">bI", payload[:5])
        assert (magic, schema_id) == (0, 4711)
        body = schemaless_reader(io.BytesIO(payload[5:]), parse_schema(SCHEMA))
        assert body == {"triggerID": "r" * 57}

    def test_a_missing_schema_id_is_refused(self):
        with pytest.raises(ValueError, match="schema id"):
            AvroSerializer(SCHEMA, None)  # pyright: ignore[reportArgumentType]


class TestDevMode:
    def test_dev_mode_frames_with_the_configured_schema_id(self):
        _, schema_id, context = resolve(dev_settings(schema_id=4711))
        assert schema_id == 4711
        assert context == {"mode": "DEV", "schema_id": 4711}

    def test_dev_mode_without_a_schema_id_refuses_to_start(self):
        with pytest.raises(PreflightError, match="schema_id"):
            resolve(dev_settings())

    def test_schema_id_overlay_is_coerced_to_int(self):
        settings = dev_settings(schema_id="4711")
        assert settings.schema_registry.schema_id == 4711
