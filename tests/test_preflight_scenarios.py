"""A failed preflight check reports the scenario its error already carried.

The registry client, the compatibility check and the token provider raise
errors classified where they happen. A check that re-derived the scenario from
the message text filed every registry failure as a schema fault, because every
registry message reads "Schema Registry ...".
"""

from __future__ import annotations

import pytest

from utility import failure_catalog as catalog
from utility import kafka_preflight as pf
from utility.error_classifier import ConnectorError, PreflightError


def failing(error: BaseException):
    def run(*_args):
        raise error

    return run


class TestSchemaRegistryCheck:
    @pytest.mark.parametrize(
        "scenario,message",
        [
            (
                catalog.SCHEMA_REGISTRY_UNAVAILABLE,
                "Schema Registry request to /subjects/t-value/versions/latest failed: Connection refused",
            ),
            (catalog.AUTHENTICATION_FAILURE, "Schema Registry returned HTTP 401 for /subjects/t-value"),
            (catalog.AUTHORISATION_FAILURE, "Schema Registry returned HTTP 403 for /subjects/t-value"),
            (catalog.SCHEMA_VALIDATION_FAILURE, "Local schema is incompatible with registered subject t-value"),
        ],
    )
    def test_the_raised_scenario_is_reported(self, scenario, message):
        report = pf.PreflightReport()
        pf.check_schema_registry(report, resolve=failing(ConnectorError(message, scenario)))

        assert report.failures[0].scenario is scenario
        with pytest.raises(PreflightError) as exc:
            report.raise_if_failed()
        assert exc.value.scenario is scenario

    def test_an_unclassified_error_is_a_registry_outage(self):
        report = pf.PreflightReport()
        pf.check_schema_registry(report, resolve=failing(RuntimeError("boom")))
        assert report.failures[0].scenario is catalog.SCHEMA_REGISTRY_UNAVAILABLE


class TestOtherChecks:
    def test_the_token_check_keeps_the_raised_scenario(self):
        report = pf.PreflightReport()
        error = PreflightError("CyberArk CCP returned HTTP 503", catalog.NETWORK_FAILURE)
        pf.check_authentication(report, acquire=failing(error))
        assert report.failures[0].scenario is catalog.NETWORK_FAILURE

    def test_the_token_check_defaults_to_authentication(self):
        report = pf.PreflightReport()
        pf.check_authentication(report, acquire=failing(RuntimeError("no token")))
        assert report.failures[0].scenario is catalog.AUTHENTICATION_FAILURE

    @pytest.mark.parametrize(
        "message,scenario",
        [
            ("KafkaError{code=TOPIC_AUTHORIZATION_FAILED}", catalog.AUTHORISATION_FAILURE),
            ("KafkaError{code=_AUTHENTICATION,str=SASL authentication error}", catalog.AUTHENTICATION_FAILURE),
            ("UNKNOWN_TOPIC_OR_PART: t is absent from cluster metadata", catalog.TOPIC_UNAVAILABLE),
            ("KafkaError{code=_TRANSPORT}", catalog.BROKER_UNAVAILABLE),
        ],
    )
    def test_metadata_failures_are_told_apart(self, message, scenario):
        report = pf.PreflightReport()
        pf.check_topic_metadata(report, fetch=failing(RuntimeError(message)), topics=["t"])
        assert report.failures[0].scenario is scenario
