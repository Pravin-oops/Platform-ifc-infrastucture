"""The two entry points in ``scripts/``.

``main.py`` (CLI) and ``main_ecs.py`` (platform) are the connector's public
surface. Each is exercised here through the path that needs no broker, no AWS
and no BSP, so the suite runs anywhere.
"""

from __future__ import annotations

import json

import pytest

from utility import failure_catalog as catalog

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


class TestMainWithoutAConfig:
    def test_it_refuses_rather_than_guessing(self, main_script, capsys):
        code = main_script.main([])

        assert code == catalog.CONTAINER_FAILURE.exit_code
        assert "--config is required" in capsys.readouterr().err

    def test_an_unreadable_config_is_classified_not_raised(self, main_script, tmp_path):
        missing = str(tmp_path / "nope.yaml")
        assert main_script.main(["--config", missing]) == catalog.CONTAINER_FAILURE.exit_code

    def test_a_run_without_a_trigger_is_refused(self, main_script, tmp_path):
        """IFC_RUN__TRIGGER picks the Athena table; without it there is nothing to read."""
        config = tmp_path / "c.yaml"
        config.write_text(
            "\n".join(
                [
                    "source: {trigger_tables: {TRIGGER_8: ifc_trigger_db.trigger_8}}",
                    "kafka: {topic: t, overrides: {bootstrap.servers: 'localhost:9092'}}",
                    "schema_registry: {mode: DEV}",
                ]
            ),
            encoding="utf-8",
        )
        assert main_script.main(["--config", str(config)]) == catalog.CONTAINER_FAILURE.exit_code


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
                    "source: {trigger_tables: {TRIGGER_8: ifc_trigger_db.trigger_8}}",
                    "kafka: {topic: t, overrides: {bootstrap.servers: 'localhost:9092'}}",
                    "schema_registry: {mode: DEV, schema_path: utility/schema.json}",
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

        result = main_ecs_script.ecs_handler({"config_path": str(config), "trigger": "TRIGGER_8"})

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