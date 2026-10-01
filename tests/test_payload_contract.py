"""The BSP payload contract and the IFC trigger field definitions."""

from __future__ import annotations

import json

import pytest

from utility import trigger_definitions as definitions
from utility.trigger_payload import (
    DataType,
    FieldSpec,
    PayloadBuildError,
    build_fields,
    serialise,
    to_payload_document,
)


def field_map(fields):
    return {f["fieldName"]: f for f in fields}


class TestFieldRendering:
    def test_decimal_is_rendered_to_two_places(self):
        spec = FieldSpec(name="Amount", source="amount", data_type=DataType.DECIMAL)
        assert build_fields([spec], {"amount": 184320.5})[0]["fieldValue"] == "184320.50"

    def test_decimal_avoids_binary_float_artefacts(self):
        spec = FieldSpec(name="Amount", source="amount", data_type=DataType.DECIMAL)
        # 0.1 + 0.2 is 0.30000000000000004 in binary floating point.
        assert build_fields([spec], {"amount": 0.1 + 0.2})[0]["fieldValue"] == "0.30"

    def test_integer_rejects_a_fractional_value_rather_than_truncating(self):
        spec = FieldSpec(name="Count", source="count", data_type=DataType.INTEGER)
        with pytest.raises(PayloadBuildError) as exc:
            build_fields([spec], {"count": 10.7})
        assert exc.value.field_name == "Count"

    def test_boolean_renders_lowercase(self):
        spec = FieldSpec(name="Flag", source="flag", data_type=DataType.BOOLEAN)
        assert build_fields([spec], {"flag": False})[0]["fieldValue"] == "false"

    def test_datetime_passes_through_an_iso_string(self):
        spec = FieldSpec(name="At", source="at", data_type=DataType.DATETIME)
        value = build_fields([spec], {"at": "2026-06-10T02:15:04.221Z"})[0]["fieldValue"]
        assert value == "2026-06-10T02:15:04.221Z"

    def test_list_transform_joins_with_a_pipe(self):
        spec = FieldSpec(
            name="Rules",
            source="rules",
            transform=lambda v: "|".join(v) if isinstance(v, list) else v,
        )
        assert build_fields([spec], {"rules": ["A", "B"]})[0]["fieldValue"] == "A|B"


class TestRequiredAndOptional:
    def test_missing_required_field_raises_and_names_the_source(self):
        spec = FieldSpec(name="Counterparty CSID", source="counterparty_csid_sds")
        with pytest.raises(PayloadBuildError) as exc:
            build_fields([spec], {})
        assert exc.value.source_key == "counterparty_csid_sds"

    def test_empty_optional_field_is_omitted_not_sent_empty(self):
        """The contract sets minLength 1 on fieldValue, so "" is invalid."""
        specs = [
            FieldSpec(name="Kept", source="kept"),
            FieldSpec(name="Dropped", source="dropped", required=False),
        ]
        fields = build_fields(specs, {"kept": "yes", "dropped": "   "})
        assert list(field_map(fields)) == ["Kept"]

    def test_default_fills_an_absent_value(self):
        spec = FieldSpec(name="Threshold", source="t", data_type=DataType.INTEGER, default=10)
        assert build_fields([spec], {})[0]["fieldValue"] == "10"

    def test_an_entirely_empty_payload_is_refused(self):
        spec = FieldSpec(name="Optional", source="missing", required=False)
        with pytest.raises(PayloadBuildError):
            build_fields([spec], {})


class TestFieldLength:
    """The consumer sizes its columns to the data-length column of the spec, so a
    value that would not fit is rejected here rather than truncated on the wire."""

    def test_a_value_at_the_limit_is_published(self):
        spec = FieldSpec(name="BRID", source="brid", max_length=10)
        assert build_fields([spec], {"brid": "B" * 10})[0]["fieldValue"] == "B" * 10

    def test_an_over_long_value_is_rejected_naming_the_field_and_the_limit(self):
        spec = FieldSpec(name="BRID", source="brid", max_length=10)
        with pytest.raises(PayloadBuildError) as exc:
            build_fields([spec], {"brid": "B" * 11})
        assert exc.value.field_name == "BRID"
        assert exc.value.source_key == "brid"
        assert "11 characters" in str(exc.value) and "at most 10" in str(exc.value)

    def test_an_over_long_optional_value_is_rejected_not_silently_dropped(self):
        """Dropping it would publish a thinner payload for bad data; the point of
        the check is that the row is looked at, not quietly thinned."""
        specs = [
            FieldSpec(name="Kept", source="kept"),
            FieldSpec(name="Long", source="long", required=False, max_length=3),
        ]
        with pytest.raises(PayloadBuildError):
            build_fields(specs, {"kept": "yes", "long": "abcd"})

    def test_the_length_is_measured_on_the_rendered_value(self):
        """A date arrives as a full timestamp and renders to ten characters, so
        the limit applies to what goes on the wire, not to the source string."""
        spec = FieldSpec(
            name="Date of Request", source="d", data_type=DataType.DATE, max_length=10
        )
        assert build_fields([spec], {"d": "2026-06-10T02:15:04.221Z"})[0]["fieldValue"] == "2026-06-10"

    def test_a_defaulted_value_is_measured_too(self):
        spec = FieldSpec(name="Location", source="loc", default="United Kingdom", max_length=5)
        with pytest.raises(PayloadBuildError):
            build_fields([spec], {})


class TestContractLengths:
    """The eight published fields carry the consumer's widths."""

    EXPECTED = {
        "Date of Request": 10,
        "Counterparty Full Legal Entity Name": 100,
        "Counterparty ID": 11,
        "Client Relationship Owner Name": 50,
        "Client Relationship Owner BRID": 10,
        "Client Relationship Owner Business Unit": 20,
        "Client Relationship Owner Location": 5,
        "Region": 50,
    }

    @pytest.mark.parametrize("definition", definitions.DEFINITIONS.values(), ids=lambda d: d.sub_type)
    def test_every_field_declares_the_specified_width(self, definition):
        assert {s.name: s.max_length for s in definition.fields} == self.EXPECTED

    def test_the_defaults_fit_their_own_fields(self):
        """A default longer than its field would quarantine every row it filled."""
        for spec in definitions.TRIGGER_8_DEFINITION.fields:
            if spec.default is not None:
                assert len(str(spec.default)) <= (spec.max_length or 0)

    def test_a_csid_widened_to_a_float_fits_the_eleven_character_field(self):
        """str(9912345678.0) is twelve characters; the transform renders the
        integer the bigint actually holds."""
        fields = build_fields(
            definitions.TRIGGER_8_DEFINITION.fields,
            {
                "date_of_request": "2026-06-10T02:18:41.907Z",
                "counterparty_csid_sds": 9912345678.0,
                "counterparty_full_legal_entity_name": "AbCdEfGh12345",
                "client_relationship_owner_name": "TOKENISED_NAME",
                "client_relationship_owner_brid": "B0433118",
                "region": "EMEA",
            },
        )
        assert field_map(fields)["Counterparty ID"]["fieldValue"] == "9912345678"


class TestPayloadValidation:
    def test_a_built_payload_satisfies_the_contract(self):
        fields = build_fields(
            definitions.TRIGGER_8_DEFINITION.fields,
            {
                "date_of_request": "2026-06-10T02:18:41.907Z",
                "counterparty_csid_sds": 9912345678,
                "counterparty_full_legal_entity_name": "AbCdEfGh12345",
                "client_relationship_owner_name": "TOKENISED_NAME",
                "client_relationship_owner_brid": "B0433118",
                "region": "EMEA",
                "business_date": "2026-06-30",
            },
        )
        # build_fields is the only thing that constructs a payload, so the
        # contract is asserted against what it produces rather than re-validated
        # per record at runtime.
        required = {"fieldName", "fieldValue", "fieldEncryptionPolicy", "fieldDataType"}
        valid_types = {t.value for t in DataType}

        assert to_payload_document(fields) == fields
        for item in fields:
            assert set(item) == required
            assert all(isinstance(v, str) for v in item.values())
            assert item["fieldValue"], "fieldValue has minLength 1; omit the field instead"
            assert item["fieldDataType"] in valid_types

    def test_an_optional_field_with_no_value_is_omitted_not_sent_empty(self):
        specs = [
            FieldSpec(name="Kept", source="kept"),
            FieldSpec(name="Dropped", source="missing", required=False),
        ]
        fields = build_fields(specs, {"kept": "v"})
        assert [f["fieldName"] for f in fields] == ["Kept"]

    def test_serialise_round_trips(self):
        fields = build_fields([FieldSpec(name="A", source="a")], {"a": "b"})
        assert json.loads(serialise(fields)) == fields

    def test_the_serialised_payload_starts_with_the_array_not_a_wrapper(self):
        fields = build_fields([FieldSpec(name="A", source="a")], {"a": "b"})
        assert serialise(fields).startswith('[{"fieldName":"A"')


class TestTriggerDefinitions:
    #: The whole published payload, in contract order. Any change to this list is
    #: a consumer-visible change and needs the BSP schema change process.
    EXPECTED_FIELDS = [
        "Date of Request",
        "Counterparty Full Legal Entity Name",
        "Counterparty ID",
        "Client Relationship Owner Name",
        "Client Relationship Owner BRID",
        "Client Relationship Owner Business Unit",
        "Client Relationship Owner Location",
        "Region",
    ]

    @pytest.mark.parametrize(
        "sub_type", [definitions.TRIGGER_8, definitions.TRIGGER_9, definitions.TRIGGER_21]
    )
    def test_every_definition_publishes_the_same_eight_fields(self, sub_type):
        """One payload contract, shared by every trigger - order included."""
        assert definitions.DEFINITIONS[sub_type].field_names() == self.EXPECTED_FIELDS

    @pytest.mark.parametrize(
        "sub_type", [definitions.TRIGGER_8, definitions.TRIGGER_9, definitions.TRIGGER_21]
    )
    def test_date_of_request_is_a_date(self, sub_type):
        spec = next(
            s for s in definitions.DEFINITIONS[sub_type].fields if s.name == "Date of Request"
        )
        assert spec.data_type is DataType.DATE

    @pytest.mark.parametrize(
        "sub_type", [definitions.TRIGGER_8, definitions.TRIGGER_9, definitions.TRIGGER_21]
    )
    def test_every_other_field_is_a_string(self, sub_type):
        for spec in definitions.DEFINITIONS[sub_type].fields:
            if spec.name == "Date of Request":
                continue
            assert spec.data_type is DataType.STRING, spec.name

    @pytest.mark.parametrize(
        "sub_type", [definitions.TRIGGER_8, definitions.TRIGGER_9, definitions.TRIGGER_21]
    )
    def test_counterparty_id_carries_the_csid_sds_value(self, sub_type):
        spec = next(
            s for s in definitions.DEFINITIONS[sub_type].fields if s.name == "Counterparty ID"
        )
        assert spec.source == "counterparty_csid_sds"
        # The sub-event identity: a row without it is not publishable.
        assert spec.required

    @pytest.mark.parametrize(
        "sub_type", [definitions.TRIGGER_8, definitions.TRIGGER_9, definitions.TRIGGER_21]
    )
    def test_no_definition_publishes_fields_the_contract_dropped(self, sub_type):
        names = definitions.DEFINITIONS[sub_type].field_names()
        for removed in (
            "Trigger_subType_detail",
            "Customer Segment",
            "Business Date",
            "Last Run Date",
            "Counterparty CSID",
            "Counterparty Legal Entity Name",
            "Relationship Owner Name",
            "TM Alert Threshold",
            "Evaluation Window Start",
            "Customer Identifier",
            "Business Unit",
            "Cluster",
            "Execution Month",
            "Detection Timestamp",
        ):
            assert removed not in names

    @pytest.mark.parametrize(
        "sub_type", [definitions.TRIGGER_8, definitions.TRIGGER_9, definitions.TRIGGER_21]
    )
    def test_field_names_are_unique_within_a_definition(self, sub_type):
        names = definitions.DEFINITIONS[sub_type].field_names()
        assert len(names) == len(set(names))

    @pytest.mark.parametrize(
        "sub_type", [definitions.TRIGGER_8, definitions.TRIGGER_9, definitions.TRIGGER_21]
    )
    def test_only_client_relationship_owner_name_is_encrypted(self, sub_type):
        """DPASS_POLICY_NAME goes on the owner name field and nowhere else."""
        for spec in definitions.DEFINITIONS[sub_type].fields:
            expected = (
                "DPASS_POLICY_NAME" if spec.name == "Client Relationship Owner Name" else ""
            )
            assert spec.encryption_policy == expected, spec.name

    def test_sub_event_discriminators_are_declared(self):
        """No table has a sub-event column, so the grain is the business date.

        business_date is read from the source row for de-duplication only; it is
        no longer part of the published payload.
        """
        for definition in definitions.DEFINITIONS.values():
            assert definition.event_key_source == "business_date"

    @pytest.mark.parametrize("alias", ["trigger_8", "Trigger8", "T8", "8", "TRIGGER_8"])
    def test_upstream_spellings_resolve(self, alias):
        assert definitions.resolve(alias).sub_type == definitions.TRIGGER_8

    def test_an_unknown_sub_type_is_refused(self):
        with pytest.raises(KeyError):
            definitions.resolve("TRIGGER_99")

    @pytest.mark.parametrize(
        "sub_type", [definitions.TRIGGER_8, definitions.TRIGGER_9, definitions.TRIGGER_21]
    )
    def test_no_definition_declares_an_empty_field_name(self, sub_type):
        """An empty fieldName would be an unencodable payload, and it is the only
        contract breach build_fields cannot detect - so it is caught here."""
        for spec in definitions.DEFINITIONS[sub_type].fields:
            assert spec.name.strip(), f"{sub_type} has a FieldSpec with no name"
            assert spec.source.strip(), f"{spec.name} has no source key"
