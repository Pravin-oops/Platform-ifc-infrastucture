"""Configuration layering and schema-drift detection."""

from __future__ import annotations

import json

import pytest
import yaml

from utility.connector_utility import load_schema_document
from utility.schema_registry_client import compare_schemas, value_subject
from utility.connector_config import ConnectorSettings, load_settings

BASE_CONFIG = {
    "source": {"table": "ifc_trigger_db.trigger_8"},
    "kafka": {"topic": "t", "bsp_config_path": "b.yaml"},
    "schema_registry": {"mode": "DEV"},
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

    def test_an_env_override_wins_over_the_yaml(self, monkeypatch, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_text(yaml.safe_dump(BASE_CONFIG), encoding="utf-8")

        monkeypatch.setenv("IFC_KAFKA__TOPIC", "overridden_topic")
        monkeypatch.setenv("IFC_RUN__SHUTDOWN_GRACE_SECONDS", "45")
        monkeypatch.setenv("IFC_HEALTH__ENABLED", "false")

        settings = load_settings(str(path), reader=lambda p: path.read_text(encoding="utf-8"))

        assert settings.kafka.topic == "overridden_topic"
        assert settings.run.shutdown_grace_seconds == 45
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


class TestSubTypeEnumDrift:
    """The enum is compared symbol by symbol, not by dict equality."""

    @pytest.fixture(scope="class")
    def local(self):
        return load_schema_document("utility/schema.json")

    @staticmethod
    def _with_symbols(schema, symbols):
        copied = json.loads(json.dumps(schema))
        for field in copied["fields"]:
            if field["name"] == "triggerSubType":
                field["type"]["symbols"] = symbols
        return copied

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

        settings = load_settings(os.path.join(app_root, "utility", "connector_config_sit.yaml"))
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
