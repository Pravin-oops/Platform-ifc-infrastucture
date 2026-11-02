"""Connectivity check mode: Secrets Manager, CyberArk, BSP and the source table are checked, nothing is published."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import time
from types import SimpleNamespace
from typing import Any, Dict

import pytest
import yaml
from botocore.exceptions import ClientError

from tests.conftest import APP_ROOT, use_config
from utility import athena_query
from utility import connectivity_check as cc
from utility import cyberark_ccp_fetch as ccp
from utility import failure_catalog as catalog
from utility import kafka_preflight as pf
from utility.auth_helper import BSPTokenProvider
from utility.connector_config import load_settings
from utility.kafka_factory import KafkaStackFactory
from utility.resilience_utility import BackoffPolicy, ShutdownSignal

pytestmark = pytest.mark.usefixtures("clean_ifc_env")

DER = b"not-really-a-certificate"
CERT = "-----BEGIN CERTIFICATE-----\n" + base64.b64encode(DER).decode() + "\n-----END CERTIFICATE-----\n"
KEY = "-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----\n"
PASSWORD = "s3cret-password"

SECRETS = {"ca-secret": CERT, "cert-secret": CERT, "key-secret": KEY}


class FakeSecrets:
    def __init__(self, values: Dict[str, str]):
        self._values = values

    def get_secret_value(self, SecretId: str) -> Dict[str, Any]:
        return {"SecretString": self._values[SecretId]}


class FakeSession:
    def __init__(self, values: Dict[str, str] = SECRETS):
        self._values = values

    def client(self, name: str, region_name: str) -> FakeSecrets:
        return FakeSecrets(self._values)


class FakeResponse:
    status_code = 200
    text = ""

    def json(self) -> Dict[str, Any]:
        return {"UserName": "svcaccount", "Content": PASSWORD}


class FakeFactory:
    instances = []

    def __init__(self, settings, **_kwargs):
        self.report = pf.PreflightReport()
        self.probed_with = "never"
        FakeFactory.instances.append(self)

    def probe(self, bsp):
        self.probed_with = bsp
        self.report.add(pf.CheckResult("tcp:kafka:quorum", True, 1.0, "1/1 endpoints reachable"))
        return {"bootstrap.servers": "broker:9093", "sasl.password": "hunter2", "oauth_cb": lambda c: None}


class FailingFactory(FakeFactory):
    def probe(self, bsp):
        self.report.add(
            pf.CheckResult("kafka:metadata:t", False, 1.0, "SASL authentication failed",
                           scenario=catalog.AUTHENTICATION_FAILURE)
        )
        return {"bootstrap.servers": "broker:9093"}


class FakeAthena:
    """Answers the table-metadata call and one COUNT query; anything else is a test failure."""

    def __init__(self, *, row_count: str = "42", denied: bool = False):
        self.row_count = row_count
        self.denied = denied
        self.queries = []

    def get_table_metadata(self, **kwargs):
        if self.denied:
            raise ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "not authorized"}}, "GetTableMetadata"
            )
        return {"TableMetadata": {"Name": kwargs["TableName"]}}

    def start_query_execution(self, **request):
        self.queries.append(request)
        return {"QueryExecutionId": "q-1"}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}, "Statistics": {}}}

    def get_paginator(self, name):
        page = {
            "ResultSet": {
                "Rows": [{"Data": [{"VarCharValue": "row_count"}]}, {"Data": [{"VarCharValue": self.row_count}]}],
                "ResultSetMetadata": {"ColumnInfo": [{"Name": "row_count", "Type": "bigint"}]},
            }
        }
        return SimpleNamespace(paginate=lambda **_kwargs: iter([page]))


def write_config(tmp_path, *, cyberark: bool = True) -> str:
    lines = [
        "app: {name: t, environment: TEST}",
        "run: {connectivity_check: true}",
        "source: {trigger_tables: {TRIGGER_8: ifc_trigger_db.trigger_8}}",
        "kafka: {topic: t, overrides: {bootstrap.servers: 'localhost:9092'}}",
        "schema_registry: {mode: DEV, schema_path: utility/schema.json}",
        "health: {enabled: false}",
        f"ca_certificate: {{secret_id: ca-secret, path: '{(tmp_path / 'ca.pem').as_posix()}'}}",
    ]
    if cyberark:
        lines.append(
            "cyberark: {base_url: 'https://ccp.example', app_id: APP, safe: SAFE, object: acct, "
            "client_cert_secret_id: cert-secret, client_key_secret_id: key-secret, ssl_verify: false}"
        )
    path = tmp_path / "c.yaml"
    path.write_text("\n".join(lines), encoding="utf-8")
    return str(path)


@pytest.fixture
def fakes(monkeypatch):
    for name in ("BSP_USERNAME", "BSP_PASSWORD", "CYBERARK_ENABLED", "CYBERARK_CCP_URL", "CYBERARK_APP_ID"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(ccp.boto3, "Session", lambda: FakeSession())
    monkeypatch.setattr(ccp.requests, "get", lambda *a, **k: FakeResponse())
    monkeypatch.setattr(ccp, "_BACKOFF", BackoffPolicy(base_seconds=0.001, max_seconds=0.001))
    monkeypatch.setattr(cc, "KafkaStackFactory", FakeFactory)
    FakeFactory.instances = []
    athena = FakeAthena()
    monkeypatch.setattr(athena_query, "athena_client", lambda: athena)
    return athena


def settings_for(tmp_path, **kwargs):
    settings = load_settings(write_config(tmp_path, **kwargs))
    settings.select_trigger("TRIGGER_8")
    return settings


def by_check(summary):
    return {r["check"]: r for r in summary["connectivity_checks"]}


class TestBundledConfigs:
    @pytest.mark.parametrize("environment", ["dev", "sit", "prod"])
    def test_every_bundled_config_runs_the_connectivity_check(self, environment):
        path = os.path.join(APP_ROOT, "utility", f"connector_config_{environment}.yaml")
        with open(path, encoding="utf-8") as handle:
            assert yaml.safe_load(handle)["run"]["connectivity_check"] is True

    def test_it_is_off_unless_configured(self, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_text(
            "source: {trigger_tables: {TRIGGER_8: d.t}}\n"
            "kafka: {topic: t, overrides: {bootstrap.servers: 'localhost:9092'}}\n"
            "schema_registry: {mode: DEV}\n",
            encoding="utf-8",
        )
        assert load_settings(str(path)).run.connectivity_check is False


class TestMasking:
    def test_mask_never_contains_the_value(self):
        masked = cc.mask(PASSWORD)
        assert PASSWORD not in masked
        assert str(len(PASSWORD)) in masked
        assert hashlib.sha256(PASSWORD.encode()).hexdigest()[:12] in masked

    def test_an_unset_value_is_called_out(self):
        assert cc.mask(None) == "<not set>"

    def test_fingerprints_match_openssl_format(self):
        expected = ":".join(f"{b:02X}" for b in hashlib.sha256(DER).digest())
        assert cc.certificate_fingerprints(CERT + CERT) == [expected, expected]

    def test_producer_config_hides_secrets_and_callables(self):
        shown = cc.redacted({"sasl.password": "hunter2", "oauth_cb": print, "acks": "all"})
        assert "hunter2" not in json.dumps(shown)
        assert shown["oauth_cb"].startswith("<callable")
        assert shown["acks"] == "all"


class TestConnectivityCheck:
    def test_all_checks_pass_and_nothing_secret_is_logged(self, fakes, tmp_path, caplog):
        caplog.set_level(logging.INFO)
        summary = cc.run(settings_for(tmp_path))

        assert summary["outcome"] == cc.OUTCOME_PASSED
        assert summary["exit_code"] == catalog.EXIT_OK
        assert summary["published"] == 0

        checks = by_check(summary)
        assert [c["status"] for c in checks.values()] == [cc.PASSED] * 5
        assert checks["source:table"]["table"] == "ifc_trigger_db.trigger_8"
        assert checks["source:table"]["row_count"] == 42
        assert checks["aws_secret:ca_certificate"]["certificates"] == 1
        assert checks["cyberark:credentials"]["username"] == "svcaccount"
        assert checks["cyberark:credentials"]["bsp_password"] == cc.mask(PASSWORD)

        logged = caplog.text + json.dumps(summary, default=str)
        assert "svcaccount" in logged
        assert PASSWORD not in logged
        assert "hunter2" not in logged
        assert "MIIE" not in logged

    def test_an_unreadable_secret_fails_the_check_but_bsp_is_still_tried(
        self, fakes, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(ccp.boto3, "Session", lambda: FakeSession({"ca-secret": CERT}))
        summary = cc.run(settings_for(tmp_path))
        checks = by_check(summary)

        assert summary["outcome"] == cc.OUTCOME_FAILED
        assert checks["aws_secret:cyberark_client_certificate"]["status"] == cc.FAILED
        assert checks["cyberark:credentials"]["status"] == cc.SKIPPED
        assert checks["bsp:kafka"]["status"] == cc.PASSED
        assert summary["exit_code"] == checks["aws_secret:cyberark_client_certificate"]["exit_code"]
        assert summary["exit_code"] != catalog.EXIT_OK

    def test_a_failed_broker_check_sets_its_exit_code(self, fakes, tmp_path, monkeypatch):
        monkeypatch.setattr(cc, "KafkaStackFactory", FailingFactory)
        summary = cc.run(settings_for(tmp_path))

        bsp = by_check(summary)["bsp:kafka"]
        assert bsp["status"] == cc.FAILED
        assert "SASL authentication failed" in bsp["detail"]
        assert summary["exit_code"] == catalog.AUTHENTICATION_FAILURE.exit_code

    def test_the_source_table_is_only_counted(self, fakes, tmp_path):
        summary = cc.run(settings_for(tmp_path))

        [query] = fakes.queries
        sql = query["QueryString"]
        assert sql.startswith("SELECT COUNT(*) AS row_count FROM ")
        assert "ifc_trigger_db" in sql and "trigger_8" in sql
        source = by_check(summary)["source:table"]
        assert source["query_execution_id"] == "q-1"
        assert "note" not in source

    def test_an_empty_month_is_reported_but_does_not_fail(self, fakes, tmp_path):
        fakes.row_count = "0"
        summary = cc.run(settings_for(tmp_path))

        source = by_check(summary)["source:table"]
        assert source["status"] == cc.PASSED
        assert source["row_count"] == 0
        assert "no rows" in source["note"]
        assert summary["exit_code"] == catalog.EXIT_OK

    def test_no_access_to_the_source_table_fails_with_the_read_exit_code(self, fakes, tmp_path):
        fakes.denied = True
        summary = cc.run(settings_for(tmp_path))
        checks = by_check(summary)

        assert checks["source:table"]["status"] == cc.FAILED
        assert "AccessDenied" in checks["source:table"]["detail"]
        assert fakes.queries == []
        assert checks["bsp:kafka"]["status"] == cc.PASSED
        assert summary["exit_code"] == catalog.BDP_READ_FAILURE.exit_code

    def test_without_cyberark_or_bsp_the_cyberark_checks_are_skipped(self, fakes, tmp_path):
        summary = cc.run(settings_for(tmp_path, cyberark=False))
        checks = by_check(summary)

        assert checks["aws_secret:cyberark_client_certificate"]["status"] == cc.SKIPPED
        assert checks["cyberark:credentials"]["status"] == cc.SKIPPED
        assert FakeFactory.instances[0].probed_with is None
        assert summary["exit_code"] == catalog.EXIT_OK


class TestProbe:
    """KafkaStackFactory.probe connects and authenticates, but never produces."""

    class Producer:
        def __init__(self, config):
            self.config = config

        def list_topics(self, topic, timeout):
            partitions = {0: object(), 1: object()}
            return SimpleNamespace(
                topics={topic: SimpleNamespace(error=None, partitions=partitions)},
                brokers={1: object()},
            )

        def poll(self, timeout):
            pass

        def produce(self, *args, **kwargs):
            raise AssertionError("probe must not produce")

    class BSP:
        claims = {"sub": "svcaccount", "iss": "bam", "exp": int(time.time()) + 3600}

        def producer_config(self, overrides):
            return {"bootstrap.servers": "broker.example:9093", **overrides}

        def token_provider(self, *, refresh_margin_seconds):
            payload = base64.urlsafe_b64encode(json.dumps(self.claims).encode()).decode().rstrip("=")
            return BSPTokenProvider(lambda: f"header.{payload}.signature")

    def test_broker_token_and_topic_are_checked(self, monkeypatch, tmp_path):
        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        monkeypatch.setattr(pf.socket, "getaddrinfo", lambda host, port: [(0, 0, 0, "", ("10.0.0.1", 0))])
        monkeypatch.setattr(pf.socket, "create_connection", lambda address, timeout: Connection())

        factory = KafkaStackFactory(
            settings_for(tmp_path), metrics=None, shutdown=ShutdownSignal(), producer_factory=self.Producer
        )
        config = factory.probe(self.BSP())

        assert config["bootstrap.servers"] == "localhost:9092"
        results = {r.name: r for r in factory.report.results}
        assert factory.report.passed
        assert "tcp:kafka:localhost:9092" in results
        assert results["auth:bam_token"].context["token_claims"]["sub"] == "svcaccount"
        assert results["kafka:metadata:t"].context["partitions"] == 2


class TestEntryPoints:
    def test_ecs_handler_checks_and_never_runs_the_connector(
        self, fakes, main_ecs_script, monkeypatch, tmp_path
    ):
        import utility.connector_runner as runner_module

        class NoRunner:
            def __init__(self, *a, **k):
                raise AssertionError("the connector must not run in connectivity check mode")

        def no_gate(*_a, **_k):
            raise AssertionError("the run and recon gates must not run in connectivity check mode")

        monkeypatch.setattr(runner_module, "ConnectorRunner", NoRunner)
        monkeypatch.setattr(main_ecs_script, "_gate", no_gate)
        monkeypatch.setattr(main_ecs_script, "_recon_gate", no_gate)
        use_config(monkeypatch, main_ecs_script, write_config(tmp_path))

        result = main_ecs_script.ecs_handler({"trigger": "TRIGGER_8"})

        assert result["outcome"] == cc.OUTCOME_PASSED
        assert result["exit_code"] == catalog.EXIT_OK
        assert result["published"] == 0

    def test_the_cli_returns_the_check_exit_code(self, fakes, main_script, monkeypatch, tmp_path):
        import utility.connector_runner as runner_module

        class NoRunner:
            def __init__(self, *a, **k):
                raise AssertionError("the connector must not run in connectivity check mode")

        monkeypatch.setattr(runner_module, "ConnectorRunner", NoRunner)
        monkeypatch.setattr(cc, "KafkaStackFactory", FailingFactory)
        monkeypatch.setenv("IFC_RUN__TRIGGER", "TRIGGER_8")
        use_config(monkeypatch, main_script, write_config(tmp_path))

        assert main_script.main([]) == catalog.AUTHENTICATION_FAILURE.exit_code
