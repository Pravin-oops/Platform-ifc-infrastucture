"""The three entry points in ``scripts/``.

``main.py`` (CLI), ``main_ecs.py`` (platform) and ``main_local.py`` (developer)
are the connector's public surface, and the restructure moved all three. Each is
exercised here through the path that needs no broker, no AWS and no BSP, so the
suite runs anywhere: the catalogue, the offline validator and the dry run.
"""

from __future__ import annotations

import json
import os

import pytest

from utility import failure_catalog as catalog
from tests.conftest import SAMPLES_DIR

pytestmark = pytest.mark.usefixtures("clean_ifc_env")


class TestCatalogueCommand:
    def test_it_exits_zero(self, main_script, capsys):
        assert main_script.main(["catalogue"]) == 0
        capsys.readouterr()

    def test_it_prints_every_registered_scenario(self, main_script, capsys):
        main_script.main(["catalogue"])
        document = json.loads(capsys.readouterr().out)

        printed = {s["key"] for s in document["scenarios"]}
        assert printed == set(catalog.SCENARIOS)

    def test_every_scenario_carries_an_actionable_row(self, main_script, capsys):
        """A catalogue entry without an owner and an exit code is not usable at 3am."""
        main_script.main(["catalogue"])
        document = json.loads(capsys.readouterr().out)

        for scenario in document["scenarios"]:
            assert scenario["incident_owner"], scenario["key"]
            assert scenario["severity"], scenario["key"]
            assert isinstance(scenario["exit_code"], int), scenario["key"]
            assert scenario["connector_behaviour"], scenario["key"]

    def test_the_exit_code_map_includes_the_two_non_failure_codes(self, main_script, capsys):
        main_script.main(["catalogue"])
        codes = json.loads(capsys.readouterr().out)["exit_codes"]

        assert codes["success"] == catalog.EXIT_OK
        assert codes["work_remaining"] == catalog.EXIT_WORK_REMAINING


class TestValidateCommand:
    def test_the_bundled_samples_all_build(self, main_script, samples_jsonl, capsys):
        assert main_script.main(["validate", "--input", samples_jsonl]) == 0

        out = capsys.readouterr().out
        assert "4 valid, 0 rejected" in out
        assert out.count("OK ") == 4

    def test_it_reports_the_trigger_id_of_each_built_record(self, main_script, samples_jsonl, capsys):
        main_script.main(["validate", "--input", samples_jsonl])
        out = capsys.readouterr().out

        for sub_type in ("NewHRCRelationship", "AccountInactivity", "MultipleTMSARs"):
            assert f"TBD-KYCRefresh-{sub_type}-" in out

    def test_show_payload_prints_the_bsp_field_list(self, main_script, samples_jsonl, capsys):
        main_script.main(["validate", "--input", samples_jsonl, "--show-payload"])
        out = capsys.readouterr().out

        assert '"fieldName"' in out
        assert '"fieldEncryptionPolicy"' in out

    def test_a_rejected_record_fails_with_the_schema_scenario_code(
        self, main_script, tmp_path, capsys
    ):
        bad = tmp_path / "bad.jsonl"
        # No counterparty CSID in the row, so idValue cannot be populated.
        bad.write_text(
            json.dumps(
                {
                    "triggerSubType": "TRIGGER_8",
                    "attributes": {},
                }
            )
            + "\n",
            encoding="utf-8",
        )

        code = main_script.main(["validate", "--input", str(bad)])

        assert code == catalog.SCHEMA_VALIDATION_FAILURE.exit_code
        assert "0 valid, 1 rejected" in capsys.readouterr().out

    def test_an_unparseable_line_is_reported_rather_than_raised(
        self, main_script, tmp_path, capsys
    ):
        bad = tmp_path / "broken.jsonl"
        bad.write_text("{not json at all\n", encoding="utf-8")

        code = main_script.main(["validate", "--input", str(bad)])

        assert code == catalog.SCHEMA_VALIDATION_FAILURE.exit_code
        assert "PARSE FAIL" in capsys.readouterr().out


class TestMainWithoutAConfig:
    def test_it_refuses_rather_than_guessing(self, main_script, capsys):
        code = main_script.main([])

        assert code == catalog.CONTAINER_FAILURE.exit_code
        assert "--config is required" in capsys.readouterr().err

    def test_an_unreadable_config_is_classified_not_raised(self, main_script, tmp_path):
        missing = str(tmp_path / "nope.yaml")
        assert main_script.main(["--config", missing]) == catalog.CONTAINER_FAILURE.exit_code


class TestLocalDryRun:
    def test_it_builds_every_bundled_sample(self, main_local_script, samples_jsonl, capsys):
        code = main_local_script.main(["--dry-run", "--input", samples_jsonl])

        out = capsys.readouterr().out
        assert code == catalog.EXIT_OK
        assert "built=4 rejected=0 oversize=0" in out

    def test_it_counts_the_records_per_sub_type(self, main_local_script, samples_jsonl, capsys):
        main_local_script.main(["--dry-run", "--input", samples_jsonl])
        summary = capsys.readouterr().out.rsplit("by_sub_type=", 1)[1].strip()

        assert json.loads(summary) == {"TRIGGER_8": 2, "TRIGGER_9": 1, "TRIGGER_21": 1}

    def test_it_reports_the_serialised_size_of_each_record(
        self, main_local_script, samples_jsonl, capsys
    ):
        main_local_script.main(["--dry-run", "--input", samples_jsonl])
        assert "bytes=" in capsys.readouterr().out

    def test_a_rejected_record_makes_the_dry_run_fail(self, main_local_script, tmp_path, capsys):
        bad = tmp_path / "bad.jsonl"
        bad.write_text(
            json.dumps(
                {
                    "triggerSubType": "TRIGGER_99",
                    "attributes": {"counterparty_csid_sds": 1},
                }
            )
            + "\n",
            encoding="utf-8",
        )

        code = main_local_script.main(["--dry-run", "--input", str(bad)])

        assert code != catalog.EXIT_OK
        assert "rejected=1" in capsys.readouterr().out

    def test_the_default_config_is_the_local_one(self, main_local_script):
        assert main_local_script.DEFAULT_CONFIG.endswith(
            os.path.join("utility", "connector_config_local.yaml")
        )
        assert os.path.isfile(main_local_script.DEFAULT_CONFIG)

    def test_an_unreadable_config_is_reported_not_raised(self, main_local_script, tmp_path):
        code = main_local_script.main(["--config", str(tmp_path / "nope.yaml"), "--dry-run"])
        assert code == catalog.CONTAINER_FAILURE.exit_code


class TestEcsEntryPoint:
    def test_it_refuses_without_a_config_path(self, main_ecs_script):
        with pytest.raises(RuntimeError, match="Missing config path"):
            main_ecs_script.ecs_handler()

    def test_the_event_config_path_is_honoured(self, main_ecs_script, tmp_path):
        """An ECS RunTask override supplies the path the same way a Lambda event does."""
        with pytest.raises(Exception) as exc:
            main_ecs_script.ecs_handler({"config_path": str(tmp_path / "absent.yaml")})
        assert "Missing config path" not in str(exc.value)

    def test_task_metadata_is_read_from_the_agent_environment(self, main_ecs_script, monkeypatch):
        monkeypatch.setenv("ECS_CLUSTER", "ifc-uat")
        monkeypatch.setenv("ECS_TASK_ARN", "arn:aws:ecs:eu-west-1:1:task/ifc-uat/abc123")
        monkeypatch.setenv("IMAGE_TAG", "0.1.0")

        task = main_ecs_script.ecs_task_metadata()

        assert task["cluster"] == "ifc-uat"
        assert task["task_arn"].endswith("abc123")
        assert task["image_tag"] == "0.1.0"

    def test_missing_agent_variables_are_none_rather_than_absent(self, main_ecs_script):
        task = main_ecs_script.ecs_task_metadata()
        assert set(task) == {
            "cluster",
            "task_arn",
            "container_name",
            "metadata_uri",
            "image_tag",
        }

    def test_a_startup_failure_is_classified_and_returned(
        self, main_ecs_script, monkeypatch, tmp_path
    ):
        """The task is about to exit; the summary is the only evidence that survives."""
        config = tmp_path / "c.yaml"
        config.write_text(
            "\n".join(
                [
                    "app: {name: t, environment: TEST}",
                    "source: {type: local, path: " + SAMPLES_DIR.replace("\\", "/") + "}",
                    "kafka: {topic: t, overrides: {bootstrap.servers: 'localhost:9092'}}",
                    "schema_registry: {mode: DEV, schema_path: utility/schema.json}",
                    "state: {backend: memory}",
                    "health: {enabled: false}",
                    "resilience: {preflight_enabled: false}",
                ]
            ),
            encoding="utf-8",
        )

        import utility.connector_runner as runner_module

        class Boom:
            def __init__(self, *a, **k):
                raise RuntimeError("Broker transport failure: no brokers available")

        monkeypatch.setattr(runner_module, "ConnectorRunner", Boom)

        result = main_ecs_script.ecs_handler({"config_path": str(config)})

        assert result["exit_code"] != 0
        assert result["scenario"]
        # The runner never started, so the startup manifest's run id is returned.
        assert result["run_id"]
        assert result["config_path"] == str(config)

    def test_main_maps_a_bootstrap_failure_onto_the_container_exit_code(
        self, main_ecs_script, monkeypatch
    ):
        monkeypatch.delenv("APP_CONFIG_PATH", raising=False)
        assert main_ecs_script.main([]) == catalog.CONTAINER_FAILURE.exit_code