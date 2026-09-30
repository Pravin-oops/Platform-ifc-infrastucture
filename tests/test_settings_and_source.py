"""Configuration layering, schema-drift detection and source parsing."""

from __future__ import annotations

import json

import pytest
import yaml

from tests.conftest import with_enum_subtype
from utility.connector_utility import load_schema_document
from utility.schema_registry_client import compare_schemas, value_subject
from utility.connector_config import ConnectorSettings, load_settings
from utility.connector_config import SourceSettings
from utility.trigger_source import ParseFailure, TriggerSource
from utility.tb_outcome_schema import TriggerEvent

BASE_CONFIG = {
    "source": {"type": "local", "path": "/tmp/x"},
    "kafka": {"topic": "t", "bsp_config_path": "b.yaml"},
    "schema_registry": {"mode": "DEV"},
    "state": {"backend": "memory"},
}


class TestSettings:
    def test_secure_mode_requires_a_registry_url(self):
        document = {**BASE_CONFIG, "schema_registry": {"mode": "SECURE"}}
        with pytest.raises(ValueError, match="schema_registry.url"):
            ConnectorSettings.model_validate(document)

    def test_writing_payloads_requires_an_audit_bucket(self):
        document = {**BASE_CONFIG, "audit": {"write_payloads": True}}
        with pytest.raises(ValueError, match="audit.bucket"):
            ConnectorSettings.model_validate(document)

    def test_service_mode_rejects_a_local_source(self):
        document = {**BASE_CONFIG, "run": {"mode": "service"}}
        with pytest.raises(ValueError, match="service"):
            ConnectorSettings.model_validate(document)

    def test_an_env_override_wins_over_the_yaml(self, monkeypatch, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_text(yaml.safe_dump(BASE_CONFIG), encoding="utf-8")

        monkeypatch.setenv("IFC_KAFKA__TOPIC", "overridden_topic")
        monkeypatch.setenv("IFC_RUN__POLL_INTERVAL_SECONDS", "900")
        monkeypatch.setenv("IFC_HEALTH__ENABLED", "false")

        settings = load_settings(str(path), reader=lambda p: path.read_text(encoding="utf-8"))

        assert settings.kafka.topic == "overridden_topic"
        assert settings.run.poll_interval_seconds == 900
        assert settings.health.enabled is False

    def test_unrelated_env_vars_are_ignored(self, monkeypatch, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_text(yaml.safe_dump(BASE_CONFIG), encoding="utf-8")
        monkeypatch.setenv("PATH_TO_SOMETHING", "x")

        settings = load_settings(str(path), reader=lambda p: path.read_text(encoding="utf-8"))
        assert settings.kafka.topic == "t"


class TestSchemaDrift:
    @pytest.fixture(scope="class")
    def local(self):
        return load_schema_document("utility/schema.json")

    def test_identical_schemas_are_compatible(self, local):
        compatible, findings = compare_schemas(local, json.loads(json.dumps(local)))
        assert compatible
        assert not [f for f in findings if f.startswith("BLOCKING")]

    def test_a_field_the_registry_does_not_know_about_blocks(self, local):
        registered = json.loads(json.dumps(local))
        registered["fields"] = [f for f in registered["fields"] if f["name"] != "triggerPostingTimestamp"]

        compatible, findings = compare_schemas(local, registered)
        assert not compatible
        assert any("triggerPostingTimestamp" in f for f in findings)

    def test_a_mandatory_registry_field_we_do_not_produce_blocks(self, local):
        registered = json.loads(json.dumps(local))
        registered["fields"].append({"name": "newMandatory", "type": "string"})

        compatible, findings = compare_schemas(local, registered)
        assert not compatible

    def test_a_new_optional_registry_field_is_informational_only(self, local):
        registered = json.loads(json.dumps(local))
        registered["fields"].append({"name": "newOptional", "type": ["null", "string"], "default": None})

        compatible, findings = compare_schemas(local, registered)
        assert compatible
        assert any("INFO" in f and "newOptional" in f for f in findings)

    def test_a_type_change_blocks(self, local):
        registered = json.loads(json.dumps(local))
        for field in registered["fields"]:
            if field["name"] == "sequenceNumber":
                field["type"] = "long"

        compatible, _ = compare_schemas(local, registered)
        assert not compatible

    def test_subject_uses_the_topic_name_strategy(self):
        assert value_subject("my_topic") == "my_topic-value"


VALID = {
    "triggerSubType": "TRIGGER_21",
    "attributes": {"counterparty_csid_sds": 8812774001,
                   "client_relationship_owner_business_unit": "UK-ICB",
                   "tmAlertCount": 9, "tmAlertThreshold": 5,
                   "breachType": "BOTH", "evaluationWindowStart": "2025-12-01",
                   "evaluationWindowEnd": "2026-05-31"},
}


class TestSource:
    def _source(self, tmp_path):
        return TriggerSource(SourceSettings(type="local", path=str(tmp_path)))

    def test_reads_json_lines(self, tmp_path):
        (tmp_path / "a.jsonl").write_text(
            json.dumps(VALID) + "\n" + json.dumps(VALID), encoding="utf-8"
        )
        items = list(self._source(tmp_path).stream())
        assert len(items) == 2
        assert all(isinstance(i, TriggerEvent) for i in items)

    def test_reads_a_single_json_object(self, tmp_path):
        (tmp_path / "a.json").write_text(json.dumps(VALID), encoding="utf-8")
        assert len(list(self._source(tmp_path).stream())) == 1

    def test_reads_a_json_array(self, tmp_path):
        (tmp_path / "a.json").write_text(json.dumps([VALID, VALID, VALID]), encoding="utf-8")
        assert len(list(self._source(tmp_path).stream())) == 3

    def test_an_array_wrapped_in_an_object_is_rejected_not_unwrapped(self, tmp_path):
        """A wrapper is read as one malformed event, not as the events inside it.

        The contract is a bare array. Asserting only the item count would pass
        here for the wrong reason - one wrapper in, one ParseFailure out - so
        assert what the item actually is.
        """
        (tmp_path / "a.json").write_text(json.dumps({"events": [VALID]}), encoding="utf-8")
        items = list(self._source(tmp_path).stream())
        assert len(items) == 1
        assert isinstance(items[0], ParseFailure)
        assert "triggerSubType" in items[0].error

    def test_malformed_json_yields_a_parse_failure_not_an_exception(self, tmp_path):
        (tmp_path / "a.jsonl").write_text("{oops}\n" + json.dumps(VALID), encoding="utf-8")
        items = list(self._source(tmp_path).stream())
        assert isinstance(items[0], ParseFailure)
        assert isinstance(items[1], TriggerEvent)

    def test_blank_lines_are_skipped(self, tmp_path):
        (tmp_path / "a.jsonl").write_text(f"\n{json.dumps(VALID)}\n\n", encoding="utf-8")
        assert len(list(self._source(tmp_path).stream())) == 1

    def test_the_batch_limit_is_honoured_when_one_is_set(self, tmp_path):
        (tmp_path / "a.jsonl").write_text(
            "\n".join(json.dumps(VALID) for _ in range(10)), encoding="utf-8"
        )
        source = TriggerSource(
            SourceSettings(type="local", path=str(tmp_path), max_records_per_batch=4)
        )
        assert len(list(source.stream())) == 4

    def test_there_is_no_record_limit_by_default(self, tmp_path):
        """The ECS contract is one object holding the whole batch: a cap would
        drop its tail with no second object to pick the rest up from."""
        (tmp_path / "a.jsonl").write_text(
            "\n".join(json.dumps(VALID) for _ in range(10_000)), encoding="utf-8"
        )
        assert SourceSettings(type="local", path=str(tmp_path)).max_records_per_batch is None
        assert len(list(self._source(tmp_path).stream())) == 10_000

    def test_an_explicit_limit_still_wins_over_no_cap(self, tmp_path):
        """``main_local.py --limit`` bounds a developer's dry run."""
        (tmp_path / "a.jsonl").write_text(
            "\n".join(json.dumps(VALID) for _ in range(10)), encoding="utf-8"
        )
        assert len(list(self._source(tmp_path).stream(limit=3))) == 3

    def test_already_processed_objects_are_skipped_on_resume(self, tmp_path):
        """Needs selection=all: under the default only one object is read, so a
        skip would be a no-op and this would pass without proving anything."""
        first = tmp_path / "a.jsonl"
        second = tmp_path / "b.jsonl"
        first.write_text(json.dumps(VALID), encoding="utf-8")
        second.write_text(json.dumps(VALID), encoding="utf-8")

        source = TriggerSource(
            SourceSettings(type="local", path=str(tmp_path), selection="all")
        )
        items = list(source.stream(skip_objects={str(first)}))
        assert len(items) == 1
        assert items[0].source_object == str(second)

    def test_the_source_object_is_recorded_for_the_audit_trail(self, tmp_path):
        (tmp_path / "a.jsonl").write_text(json.dumps(VALID), encoding="utf-8")
        source = self._source(tmp_path)
        list(source.stream())
        assert source.objects_read == [str(tmp_path / "a.jsonl")]


class TestTheSingleFileContract:
    """ECS is handed one .json object holding the whole batch as an array.

    ``source.path`` names that object rather than a prefix, which the path
    iterator already supports - these tests pin the behaviour so it cannot
    regress into prefix-only listing.
    """

    def _rows(self, n):
        rows = []
        for i in range(n):
            row = json.loads(json.dumps(VALID))
            row["attributes"]["counterparty_csid_sds"] = 9900000000 + i
            rows.append(row)
        return rows

    def test_a_path_naming_one_file_reads_only_that_file(self, tmp_path):
        target = tmp_path / "batch.json"
        target.write_text(json.dumps(self._rows(3)), encoding="utf-8")
        # A decoy that a prefix read would also pick up.
        (tmp_path / "other.json").write_text(json.dumps(self._rows(5)), encoding="utf-8")

        source = TriggerSource(SourceSettings(type="local", path=str(target)))
        items = list(source.stream())

        assert len(items) == 3
        assert all(isinstance(i, TriggerEvent) for i in items)
        assert source.objects_read == [str(target)]

    def test_every_record_in_the_file_is_published(self, tmp_path):
        """The case the cap used to break: a batch larger than the old default."""
        target = tmp_path / "batch.json"
        target.write_text(json.dumps(self._rows(6_000)), encoding="utf-8")

        items = list(TriggerSource(SourceSettings(type="local", path=str(target))).stream())
        assert len(items) == 6_000
        assert not any(isinstance(i, ParseFailure) for i in items)

    def test_each_record_keeps_its_index_within_the_file(self, tmp_path):
        """With one object, the index is the only thing locating a record."""
        target = tmp_path / "batch.json"
        target.write_text(json.dumps(self._rows(4)), encoding="utf-8")

        items = list(TriggerSource(SourceSettings(type="local", path=str(target))).stream())
        assert [i.source_index for i in items] == [0, 1, 2, 3]
        assert {i.source_object for i in items} == {str(target)}

    def test_one_bad_record_does_not_cost_the_rest_of_the_file(self, tmp_path):
        target = tmp_path / "batch.json"
        rows = self._rows(3)
        rows[1] = {"triggerType": "IFC_CDD"}  # no triggerSubType
        target.write_text(json.dumps(rows), encoding="utf-8")

        items = list(TriggerSource(SourceSettings(type="local", path=str(target))).stream())
        assert [type(i).__name__ for i in items] == [
            "TriggerEvent", "ParseFailure", "TriggerEvent"
        ]

    def test_a_malformed_array_costs_the_whole_batch(self, tmp_path):
        """The cost of consolidating: there is no second object to fall back on.

        Documented rather than fixed - .jsonl from upstream is what makes a
        batch degrade per record instead of all at once.
        """
        target = tmp_path / "batch.json"
        target.write_text(json.dumps(self._rows(500))[:-1], encoding="utf-8")  # truncated

        items = list(TriggerSource(SourceSettings(type="local", path=str(target))).stream())
        assert len(items) == 1
        assert isinstance(items[0], ParseFailure)
        assert "not valid JSON" in items[0].error

    def test_a_missing_local_file_raises_a_source_access_error(self, tmp_path):
        """Locally the path iterator checks existence and raises."""
        from utility.connector_utility import SourceAccessError

        source = TriggerSource(SourceSettings(type="local", path=str(tmp_path / "absent.json")))
        with pytest.raises(SourceAccessError):
            list(source.stream())

    def test_the_file_is_still_found_when_the_path_names_its_folder(self, tmp_path):
        """Pointing at the prefix keeps working, so the config change is reversible."""
        (tmp_path / "batch.json").write_text(json.dumps(self._rows(3)), encoding="utf-8")
        assert len(list(TriggerSource(SourceSettings(type="local", path=str(tmp_path))).stream())) == 3


class TestSubTypeEnumDrift:
    """The enum is compared symbol by symbol, not by dict equality."""

    @pytest.fixture(scope="class")
    def local(self):
        return with_enum_subtype(load_schema_document("utility/schema.json"))

    @staticmethod
    def _with_symbols(schema, symbols):
        return with_enum_subtype(schema, symbols)

    def test_the_bundled_string_schema_blocks_against_a_registered_enum(self):
        """Publishing the symbol as text needs the registered subject changed first.

        The producer writes with the local schema under the registered schema id,
        so a consumer would decode the string's length prefix as an enum index.
        """
        bundled = load_schema_document("utility/schema.json")
        compatible, findings = compare_schemas(bundled, with_enum_subtype(bundled))
        assert not compatible
        assert any("triggerSubType" in f and "enum on one side only" in f for f in findings)

    def test_the_bundled_string_schema_matches_a_registered_string(self):
        bundled = load_schema_document("utility/schema.json")
        compatible, _ = compare_schemas(bundled, json.loads(json.dumps(bundled)))
        assert compatible

    def test_symbol_order_and_docs_do_not_count_as_drift(self, local):
        registered = self._with_symbols(
            local, ["SigChanges", "UBOChanges", "MultipleTMSARs", "AccountInactivity",
                    "NewHRCRelationship"]
        )
        for field in registered["fields"]:
            if field["name"] == "triggerSubType":
                field["type"]["doc"] = "added by the registry"

        compatible, findings = compare_schemas(local, registered)
        assert compatible
        assert not [f for f in findings if f.startswith("BLOCKING")]

    def test_a_symbol_we_publish_that_the_registry_lacks_blocks(self, local):
        registered = self._with_symbols(local, ["NewHRCRelationship", "UBOChanges", "SigChanges"])

        compatible, findings = compare_schemas(local, registered)
        assert not compatible
        assert any("AccountInactivity" in f and f.startswith("BLOCKING") for f in findings)

    def test_extra_registry_symbols_are_informational(self, local):
        registered = self._with_symbols(
            local,
            ["NewHRCRelationship", "AccountInactivity", "MultipleTMSARs", "UBOChanges",
             "SigChanges", "SomethingNew"],
        )

        compatible, findings = compare_schemas(local, registered)
        assert compatible
        assert any("SomethingNew" in f for f in findings)

    def test_the_registry_reverting_to_a_plain_string_blocks(self, local):
        registered = json.loads(json.dumps(local))
        for field in registered["fields"]:
            if field["name"] == "triggerSubType":
                field["type"] = "string"

        compatible, _ = compare_schemas(local, registered)
        assert not compatible

class TestTheAuditLocation:
    """``audit.bucket`` accepts a bare bucket name or a full s3:// URI.

    The URI form is what breaks naively: the writer used to compose
    ``f"s3://{bucket}/..."``, so a configured ``s3://...`` value produced
    ``s3://s3://...`` and every write went to a path that does not exist.
    """

    def _base(self, bucket, prefix="manifests/"):
        from utility.audit_utility import AuditWriter
        from utility.connector_config import AuditSettings

        settings = AuditSettings(bucket=bucket)
        return AuditWriter(settings, run_id="r", environment="TEST")._base(prefix)

    def test_a_bare_bucket_name_gains_the_scheme(self):
        assert self._base("my-bucket") == "s3://my-bucket/manifests"

    def test_a_uri_is_not_given_the_scheme_twice(self):
        assert self._base("s3://my-bucket") == "s3://my-bucket/manifests"

    def test_a_uri_naming_a_folder_nests_the_prefix_under_it(self):
        assert (
            self._base("s3://my-bucket/team/audit")
            == "s3://my-bucket/team/audit/manifests"
        )

    @pytest.mark.parametrize(
        "written", ["s3://my-bucket/audit/", "  s3://my-bucket/audit  ", "s3://my-bucket/audit"]
    )
    def test_trailing_slashes_and_space_do_not_double_up(self, written):
        assert self._base(written) == "s3://my-bucket/audit/manifests"

    def test_the_raw_value_is_kept_for_the_log(self):
        from utility.connector_config import AuditSettings

        settings = AuditSettings(bucket="s3://my-bucket/audit/")
        assert settings.bucket == "s3://my-bucket/audit"
        assert settings.root == "s3://my-bucket/audit"

    def test_an_unset_bucket_disables_the_writer_rather_than_raising(self):
        from utility.audit_utility import AuditWriter
        from utility.connector_config import AuditSettings

        writer = AuditWriter(AuditSettings(), run_id="r", environment="TEST")
        assert not writer.enabled

    def test_the_deployed_config_resolves_to_the_audit_folder(self, app_root):
        import os

        from utility.audit_utility import AuditWriter
        from utility.connector_config import load_settings

        settings = load_settings(os.path.join(app_root, "utility", "connector_config.yaml"))
        writer = AuditWriter(settings.audit, run_id="r", environment="TEST")

        for prefix in (
            settings.audit.manifest_prefix,
            settings.audit.quarantine_prefix,
            settings.audit.payload_prefix,
        ):
            base = writer._base(prefix)
            assert base.startswith("s3://sit1-pre-cds01-509153454187-eu-west-1/")
            assert base.count("s3://") == 1
            assert "/audit-bucket/" in base
