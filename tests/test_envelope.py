"""Envelope construction, identity and Avro validation."""

from __future__ import annotations

import json
import re
from datetime import date

import pytest

from utility import tb_outcome_schema
from utility import trigger_definitions as definitions
from utility.error_classifier import PreflightError, RecordRejected
from utility.connector_utility import load_schema_document
from utility.tb_outcome_schema import (
    CSID_SOURCE,
    EnvelopeBuilder,
    TriggerEvent,
    month_end_timestamp,
    previous_month,
)

#: RFC 3339, UTC, nanosecond precision - the shape of both envelope timestamps.
RFC3339_NANOS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z$")


@pytest.fixture(scope="module")
def schema():
    return load_schema_document("utility/schema.json")


def make_builder(schema, **overrides):
    options = dict(
        avro_schema=schema,
        business_month="2026-06",
    )
    options.update(overrides)
    return EnvelopeBuilder(**options)


@pytest.fixture
def builder(schema):
    return make_builder(schema)


def trigger_8_row(**overrides):
    """One bdp_corp_ifc_trigger_8 row, keyed by the table's column names."""
    row = {
        "date_of_request": "2026-06-10T02:15:04.221Z",
        "counterparty_full_legal_entity_name": "AbCdEfGh12345",
        "counterparty_csid_sds": 9912345678,
        "customer_segment": "Corporate",
        "client_relationship_owner_brid": "B0412775",
        "client_relationship_owner_name": "XyZwVu67890",
        "client_relationship_owner_business_unit": "UK Corporate",
        "client_relationship_owner_location": "UK",
        "region": "EMEA",
        "business_date": "2026-06-30",
    }
    row.update(overrides)
    return row


def trigger_8_event(attributes=None, **wrapper):
    """A Trigger 8 event in the shape the source yields."""
    raw = {
        "triggerType": "IFC_CDD",
        "triggerSubType": "TRIGGER_8",
        "attributes": trigger_8_row() if attributes is None else attributes,
    }
    raw.update(wrapper)
    return TriggerEvent.from_dict(raw)


class TestEnvelope:
    def test_builds_a_schema_valid_record(self, builder):
        record = builder.build(trigger_8_event()).record
        assert record["triggerSubType"] == "NewHRCRelationship"
        assert isinstance(record["sequenceNumber"], int)

    def test_envelope_constants_are_published_whatever_the_input_says(self, builder):
        built = builder.build(trigger_8_event(triggerType="SOMETHING_ELSE"))
        record = built.record
        assert record["triggerType"] == "KYCRefresh"
        assert record["triggerOriginatingSystem"] == "TBD"
        assert record["triggerOriginatingBU"] == "UK-C and UK-ICB"
        assert record["idType"] == "Customer"
        assert record["idSystem"] == "Corelation id"
        assert built.trigger_id.startswith("TBD-KYCRefresh-NewHRCRelationship-")

    def test_only_client_relationship_owner_name_carries_an_encryption_policy(self, builder):
        fields = builder.build(trigger_8_event()).payload_fields
        policies = {f["fieldName"]: f["fieldEncryptionPolicy"] for f in fields}
        assert policies.pop("Client Relationship Owner Name") == "DPASS_POLICY_NAME"
        assert set(policies.values()) == {""}

    def test_the_payload_is_the_eight_contract_fields_in_order(self, builder):
        fields = builder.build(trigger_8_event()).payload_fields
        assert [f["fieldName"] for f in fields] == [
            "Date of Request",
            "Counterparty Full Legal Entity Name",
            "Counterparty ID",
            "Client Relationship Owner Name",
            "Client Relationship Owner BRID",
            "Client Relationship Owner Business Unit",
            "Client Relationship Owner Location",
            "Region",
        ]

    def test_date_of_request_is_published_as_a_date_not_a_timestamp(self, builder):
        fields = {f["fieldName"]: f for f in builder.build(trigger_8_event()).payload_fields}
        assert fields["Date of Request"]["fieldValue"] == "2026-06-10"
        assert fields["Date of Request"]["fieldDataType"] == "Date"

    def test_id_type_is_customer_and_id_value_is_the_csid(self, builder):
        record = builder.build(trigger_8_event()).record
        assert record["idType"] == "Customer"
        assert record["idValue"] == "9912345678"

    def test_a_csid_widened_to_a_float_is_published_as_an_integer(self, builder):
        event = trigger_8_event(attributes=trigger_8_row(counterparty_csid_sds=9912345678.0))
        assert builder.build(event).record["idValue"] == "9912345678"

    @pytest.mark.parametrize("business_unit", ["UK-ICB", None, ""])
    def test_originating_bu_is_constant_whatever_the_rows_business_unit(self, builder, business_unit):
        event = trigger_8_event(
            attributes=trigger_8_row(client_relationship_owner_business_unit=business_unit)
        )
        assert builder.build(event).record["triggerOriginatingBU"] == "UK-C and UK-ICB"

    def test_timestamp_is_the_last_instant_of_the_business_month(self, builder):
        record = builder.build(trigger_8_event()).record
        assert record["timestamp"] == "2026-06-30T23:59:59.999999999Z"

    def test_every_record_in_a_run_carries_the_same_timestamp(self, builder):
        first = builder.build(trigger_8_event()).record
        other = builder.build(
            trigger_8_event(
                attributes=trigger_8_row(counterparty_csid_sds=4471002233, business_date="2026-06-12")
            )
        ).record
        assert first["timestamp"] == other["timestamp"]

    def test_both_timestamps_are_rfc3339_with_nanosecond_precision(self, builder):
        record = builder.build(trigger_8_event()).record
        assert RFC3339_NANOS.match(record["timestamp"])
        assert RFC3339_NANOS.match(record["triggerPostingTimestamp"])
        assert record["triggerPostingTimestamp"] != record["timestamp"]

    def test_payload_is_a_json_string_not_an_object(self, builder):
        record = builder.build(trigger_8_event()).record
        assert isinstance(record["payload"], str)
        document = json.loads(record["payload"])
        # The array itself, not a {"payload": [...]} wrapper inside the payload field.
        assert isinstance(document, list)
        assert {"fieldName", "fieldValue", "fieldEncryptionPolicy", "fieldDataType"} == set(document[0])

    def test_partition_key_is_the_csid_so_related_events_stay_ordered(self, builder):
        assert builder.build(trigger_8_event()).kafka_key == "9912345678"

    def test_identity_keys_in_the_wrapper_are_ignored(self, builder):
        """Identity comes from the row and the run, never from the wrapper."""
        event = trigger_8_event(
            customerId="IGNORED",
            idType="CustomerID",
            executionMonth="1999-01",
            detectionTimestamp="1999-01-01T00:00:00.000Z",
            businessUnit="Somewhere else",
        )
        record = builder.build(event).record
        assert record["idValue"] == "9912345678"
        assert record["idType"] == "Customer"
        assert record["timestamp"] == "2026-06-30T23:59:59.999999999Z"
        assert record["triggerOriginatingBU"] == "UK-C and UK-ICB"


class TestBusinessMonth:
    @pytest.mark.parametrize(
        "run_date,expected",
        [
            (date(2026, 7, 15), "2026-06"),
            (date(2026, 1, 3), "2025-12"),
            (date(2026, 3, 31), "2026-02"),
        ],
    )
    def test_the_business_month_is_the_month_before_the_run(self, run_date, expected):
        assert previous_month(run_date) == expected

    @pytest.mark.parametrize(
        "month,expected",
        [
            ("2026-06", "2026-06-30T23:59:59.999999999Z"),
            ("2026-02", "2026-02-28T23:59:59.999999999Z"),
            ("2028-02", "2028-02-29T23:59:59.999999999Z"),
            ("2025-12", "2025-12-31T23:59:59.999999999Z"),
        ],
    )
    def test_month_end_timestamp(self, month, expected):
        assert month_end_timestamp(month) == expected

    def test_the_builder_defaults_to_the_month_before_today(self, schema, monkeypatch):
        monkeypatch.setattr(tb_outcome_schema, "today", lambda: date(2026, 7, 15))
        builder = make_builder(schema, business_month=None)

        assert builder.business_month == "2026-06"
        assert builder.build(trigger_8_event()).record["timestamp"] == "2026-06-30T23:59:59.999999999Z"

    @pytest.mark.parametrize("bad", ["2026-13", "June", "2026-6"])
    def test_a_malformed_business_month_is_refused_at_construction(self, schema, bad):
        with pytest.raises(ValueError):
            make_builder(schema, business_month=bad)


class TestIdentity:
    def test_trigger_id_is_deterministic_across_builders(self, schema):
        first = make_builder(schema).build(trigger_8_event()).trigger_id
        second = make_builder(schema).build(trigger_8_event()).trigger_id
        assert first == second

    def test_the_trigger_id_names_the_business_month(self, builder):
        assert "-2026-06-" in builder.build(trigger_8_event()).trigger_id

    def test_sub_events_for_one_counterparty_get_distinct_ids(self, builder):
        """Two business dates for one counterparty in a month are two triggers.

        The BDP table's grain is one row per counterparty per business date, so
        business_date is what separates them - there is no country or account
        column to discriminate on.
        """
        june_30 = builder.build(trigger_8_event())
        june_15 = builder.build(trigger_8_event(attributes=trigger_8_row(business_date="2026-06-15")))
        assert june_30.trigger_id != june_15.trigger_id

    def test_the_business_key_carries_the_csid_month_and_business_date(self, builder):
        event = trigger_8_event()
        key = json.loads(builder.business_key(event, definitions.TRIGGER_8_DEFINITION))

        assert key["idType"] == "Customer"
        assert key["idValue"] == "9912345678"
        assert key["businessMonth"] == "2026-06"
        assert key["eventKey"] == "2026-06-30"

    def test_a_different_counterparty_is_a_different_trigger(self, builder):
        one = builder.build(trigger_8_event()).trigger_id
        other = builder.build(trigger_8_event(attributes=trigger_8_row(counterparty_csid_sds=9912345679))).trigger_id
        assert one != other

    def test_a_different_business_month_is_a_different_trigger(self, schema):
        june = make_builder(schema).build(trigger_8_event()).trigger_id
        july = make_builder(schema, business_month="2026-07").build(trigger_8_event()).trigger_id
        assert june != july

    def test_a_customers_second_event_is_numbered_and_identified_separately(self, builder):
        """Two events from one source for one customer are two events, not one
        seen twice: the occurrence separates both the number and the identity."""
        first = builder.build(trigger_8_event()).record
        second = builder.build(trigger_8_event()).record

        assert (first["sequenceNumber"], second["sequenceNumber"]) == (1, 2)
        assert first["triggerID"] != second["triggerID"]


class TestRejection:
    def test_unknown_sub_type_is_rejected_not_raised_as_a_run_failure(self, builder):
        event = trigger_8_event(triggerSubType="TRIGGER_99")
        with pytest.raises(RecordRejected) as exc:
            builder.build(event)
        assert exc.value.scenario.key == "SCHEMA_VALIDATION_FAILURE"

    @pytest.mark.parametrize("csid", [None, "", "   ", 12.5])
    def test_a_row_without_a_usable_csid_is_rejected_naming_id_value(self, builder, csid):
        event = trigger_8_event(attributes=trigger_8_row(counterparty_csid_sds=csid))
        with pytest.raises(RecordRejected) as exc:
            builder.build(event)
        assert exc.value.detail["field_name"] == "idValue"
        assert exc.value.detail["source_key"] == CSID_SOURCE

    def test_a_row_without_a_business_date_still_publishes(self, builder):
        """business_date feeds the de-duplication key only; it is not a payload field."""
        row = trigger_8_row()
        del row["business_date"]
        fields = builder.build(trigger_8_event(attributes=row)).payload_fields
        assert "Business Date" not in {f["fieldName"] for f in fields}

    @pytest.mark.parametrize(
        "source",
        [
            "date_of_request",
            "counterparty_full_legal_entity_name",
            "client_relationship_owner_name",
            "client_relationship_owner_brid",
            "region",
        ],
    )
    def test_a_row_missing_a_mandatory_field_is_rejected(self, builder, source):
        """All eight payload fields are mandatory; the two with contract defaults
        are covered separately below. A row missing any of the rest is
        quarantined rather than published with a hole."""
        row = trigger_8_row()
        del row[source]
        with pytest.raises(RecordRejected) as exc:
            builder.build(trigger_8_event(attributes=row))
        assert exc.value.detail["source_key"] == source

    @pytest.mark.parametrize(
        "source, expected",
        [
            ("client_relationship_owner_business_unit", "UK Corporate"),
            ("client_relationship_owner_location", "UK"),
        ],
    )
    def test_a_defaulted_field_is_filled_in_rather_than_rejected(self, builder, source, expected):
        row = trigger_8_row()
        del row[source]
        fields = builder.build(trigger_8_event(attributes=row)).payload_fields
        spec = next(s for s in definitions.TRIGGER_8_DEFINITION.fields if s.source == source)
        assert {f["fieldName"]: f["fieldValue"] for f in fields}[spec.name] == expected

    @pytest.mark.parametrize(
        "source, value",
        [
            ("counterparty_full_legal_entity_name", "N" * 101),
            ("client_relationship_owner_name", "R" * 51),
            ("client_relationship_owner_brid", "B" * 11),
            ("client_relationship_owner_business_unit", "U" * 21),
            ("client_relationship_owner_location", "London"),
            ("region", "E" * 51),
        ],
    )
    def test_a_value_wider_than_the_consumers_column_is_rejected(self, builder, source, value):
        event = trigger_8_event(attributes=trigger_8_row(**{source: value}))
        with pytest.raises(RecordRejected) as exc:
            builder.build(event)
        assert exc.value.scenario.key == "SCHEMA_VALIDATION_FAILURE"
        assert exc.value.detail["source_key"] == source

    def test_missing_trigger_sub_type_is_rejected_at_parse_time(self):
        with pytest.raises(RecordRejected) as exc:
            TriggerEvent.from_dict({"attributes": trigger_8_row()})
        assert "triggerSubType" in str(exc.value)

    def test_non_object_attributes_are_rejected(self):
        with pytest.raises(RecordRejected):
            TriggerEvent.from_dict({"triggerSubType": "TRIGGER_8", "attributes": []})

    def test_avro_validation_failure_names_the_offending_field(self, builder):
        record = builder.build(trigger_8_event()).record
        record["sequenceNumber"] = "not-an-int"

        with pytest.raises(RecordRejected) as exc:
            builder.validate(record)
        assert any("sequenceNumber" in p for p in exc.value.detail["problems"])


class TestAllTriggers:
    def test_trigger_9(self, builder):
        event = TriggerEvent.from_dict(
            {
                "triggerSubType": "TRIGGER_9",
                "attributes": {
                    "date_of_request": "2026-06-10T02:18:41.907Z",
                    "counterparty_full_legal_entity_name": "LmNoPqRs99887",
                    "counterparty_csid_sds": 4471002233,
                    "client_relationship_owner_name": "TOKENISED_NAME",
                    "client_relationship_owner_brid": "B0433118",
                    "client_relationship_owner_business_unit": "UK Corporate",
                    "region": "EMEA",
                    "business_date": "2026-06-30",
                    "last_run_date": "2026-05-31",
                },
            }
        )
        built = builder.build(event)
        fields = {f["fieldName"]: f["fieldValue"] for f in built.payload_fields}
        assert built.record["idValue"] == "4471002233"
        assert fields["Counterparty ID"] == "4471002233"
        # last_run_date is no longer published, though the source row still carries it.
        assert "Last Run Date" not in fields

    def test_trigger_21(self, builder):
        event = TriggerEvent.from_dict(
            {
                "triggerSubType": "TRIGGER_21",
                "attributes": {
                    "date_of_request": "2026-06-10T02:20:11.004Z",
                    "counterparty_full_legal_entity_name": "ZzYyXx11223",
                    "counterparty_csid_sds": 8812774001,
                    "client_relationship_owner_name": "PqRsTu09876",
                    "client_relationship_owner_brid": "B0440021",
                    "client_relationship_owner_business_unit": "UK-ICB",
                    "client_relationship_owner_location": "UK",
                    "region": "EMEA",
                    "business_date": "2026-06-30",
                },
            }
        )
        built = builder.build(event)
        fields = {f["fieldName"]: f["fieldValue"] for f in built.payload_fields}
        assert built.record["idValue"] == "8812774001"
        # Trigger 21 publishes the same eight fields as Triggers 8 and 9.
        assert fields["Counterparty ID"] == "8812774001"
        assert fields["Client Relationship Owner Location"] == "UK"
        assert len(fields) == 8


class TestSubTypeEnum:
    """triggerSubType is an Avro enum, so the published spelling is load-bearing."""

    def test_the_published_symbol_is_written_not_the_internal_key(self, builder):
        record = builder.build(trigger_8_event()).record
        assert record["triggerSubType"] == definitions.SUBTYPE_NEW_HRC_RELATIONSHIP
        assert record["triggerSubType"] != definitions.TRIGGER_8

    @pytest.mark.parametrize(
        "internal,published",
        [
            ("TRIGGER_8", "NewHRCRelationship"),
            ("TRIGGER_9", "AccountInactivity"),
            ("TRIGGER_21", "MultipleTMSARs"),
        ],
    )
    def test_every_definition_maps_to_a_registered_symbol(self, schema, internal, published):
        symbols = _subtype_symbols(schema)
        assert definitions.DEFINITIONS[internal].published_sub_type == published
        assert published in symbols

    def test_the_published_symbol_also_resolves_on_input(self):
        assert definitions.resolve("NewHRCRelationship").sub_type == definitions.TRIGGER_8

    def test_a_symbol_we_do_not_produce_is_rejected_as_out_of_scope(self, builder):
        with pytest.raises(RecordRejected) as exc:
            builder.build(trigger_8_event(triggerSubType="UBOChanges"))
        assert "not produced by this connector" in str(exc.value)

    def test_the_trigger_id_carries_the_published_symbol(self, builder):
        built = builder.build(trigger_8_event())
        assert "-NewHRCRelationship-" in built.trigger_id

    def test_a_schema_missing_one_of_our_symbols_aborts_at_construction(self, schema):
        narrowed = json.loads(json.dumps(schema))
        for field in narrowed["fields"]:
            if field["name"] == "triggerSubType":
                field["type"]["symbols"] = ["NewHRCRelationship"]

        with pytest.raises(PreflightError) as exc:
            make_builder(narrowed)
        assert "AccountInactivity" in str(exc.value)

    def test_a_plain_string_schema_still_works(self, schema):
        relaxed = json.loads(json.dumps(schema))
        for field in relaxed["fields"]:
            if field["name"] == "triggerSubType":
                field["type"] = "string"

        builder = make_builder(relaxed)
        assert builder.build(trigger_8_event()).record["triggerSubType"] == "NewHRCRelationship"


def _subtype_symbols(schema):
    for field in schema["fields"]:
        if field["name"] == "triggerSubType":
            return set(field["type"]["symbols"])
    raise AssertionError("triggerSubType not in schema")
