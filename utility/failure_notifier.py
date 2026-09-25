"""Turn a classification into an RTB-ready alert.

The subject line alone has to be enough for the on-call engineer to route the
incident, because that is all a paging integration shows. The body then answers,
in order: what broke, what the connector already did about it, who owns it, and
what to do next - all lifted from the agreed catalogue rather than invented here.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from utility.error_classifier import Classification

logger = logging.getLogger(__name__)

_MAX_SUBJECT = 100  # SNS subject limit.


def build_subject(
    classification: Classification,
    *,
    application: str = "IFC Trigger Connector",
    environment: Optional[str] = None,
) -> str:
    prefix = f"[{environment}] " if environment else ""
    subject = (
        f"{prefix}{classification.scenario.severity.value} - {application} - "
        f"{classification.scenario.error_type}"
    )
    return subject[:_MAX_SUBJECT]


def build_body(
    classification: Classification,
    *,
    run_context: Optional[Dict[str, Any]] = None,
) -> str:
    scenario = classification.scenario
    run_context = run_context or {}

    lines = [
        f"Scenario        : {scenario.key} ({scenario.layer.value})",
        f"Error type      : {scenario.error_type}",
        f"Severity        : {scenario.severity.value}",
        f"Retryable       : {scenario.retryable}",
        f"Producer fix    : {'required' if scenario.producer_fix_required else 'not required'}",
        f"Exit code       : {scenario.exit_code}",
        "",
        "WHAT HAPPENED",
        f"  {scenario.error_details}",
        f"  Likely causes: {scenario.causes}",
        f"  Impact: {scenario.impact}",
        "",
        "WHAT THE CONNECTOR DID",
        f"  Handling: {scenario.handling.value}",
        f"  {scenario.connector_behaviour}",
        "",
        "OWNERSHIP",
        f"  Raise incident with : {scenario.incident_owner.value}",
        f"  Inform              : {', '.join(o.value for o in scenario.inform) or 'n/a'}",
        "",
        "ACTIONS",
        f"  Producer RTB : {scenario.producer_rtb_action}",
        f"  BSP RTB      : {scenario.bsp_rtb_action}",
        "",
        "RUN CONTEXT",
    ]

    for key, value in sorted({**run_context, **(classification.context or {})}.items()):
        lines.append(f"  {key} = {value}")

    lines += [
        f"  operation = {classification.operation}",
        f"  topic = {classification.topic}",
        "",
        "RAW ERROR",
        f"  {classification.raw_error[:2000]}",
    ]

    if classification.kafka_error_name:
        lines.append(
            f"  kafka_error = {classification.kafka_error_name} (code={classification.kafka_error_code})"
        )

    return "\n".join(lines)


def _describe_error(exc: Exception) -> str:
    """``AuthorizationError: User ... is not authorized`` rather than a bare class name."""
    if isinstance(exc, ClientError):
        error = exc.response.get("Error", {})
        return f"{error.get('Code', 'ClientError')}: {error.get('Message', exc)}"
    return f"{type(exc).__name__}: {exc}"


class Notifier:
    """Publishes alerts to SNS when configured; always logs them.

    A failure to notify is never allowed to mask the failure being notified, so
    SNS errors are logged and swallowed.
    """

    def __init__(
        self,
        *,
        sns_topic_arn: Optional[str] = None,
        application: str = "IFC Trigger Connector",
        environment: Optional[str] = None,
        client: Any = None,
    ):
        self._topic_arn = sns_topic_arn
        self._application = application
        self._environment = environment
        self._client = client

    def _sns(self) -> Any:
        if self._client is None:
            self._client = boto3.client("sns")
        return self._client

    def notify(
        self,
        classification: Classification,
        *,
        run_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        subject = build_subject(
            classification, application=self._application, environment=self._environment
        )
        body = build_body(classification, run_context=run_context)

        logger.error(
            "ALERT %s",
            subject,
            extra={
                "alert_subject": subject,
                "classification": classification.to_dict(),
                "run_context": run_context or {},
            },
        )

        attributes = {
            "scenario": classification.scenario.key,
            "severity": classification.scenario.severity.value,
            "owner": classification.scenario.incident_owner.value,
        }
        sns_fields = {
            "sns_kind": "failure_alert",
            "sns_topic_arn": self._topic_arn,
            "sns_subject": subject,
            "sns_message": body,
            "sns_message_attributes": attributes,
        }

        if not self._topic_arn:
            logger.warning(
                "SNS failure alert NOT sent (no topic configured); subject=%r message=\n%s",
                subject,
                body,
                extra=sns_fields,
            )
            return {"subject": subject, "message": body}

        logger.info(
            "SNS failure alert sending: topic=%s subject=%r message=\n%s",
            self._topic_arn,
            subject,
            body,
            extra=sns_fields,
        )

        try:
            response = self._sns().publish(
                TopicArn=self._topic_arn,
                Subject=subject,
                Message=body,
                MessageAttributes={
                    name: {"DataType": "String", "StringValue": value}
                    for name, value in attributes.items()
                },
            )
        except (ClientError, BotoCoreError) as exc:
            error = _describe_error(exc)
            logger.exception(
                "SNS failure alert FAILED to send; alert remains in the log only: "
                "topic=%s error=%s subject=%r message=\n%s",
                self._topic_arn,
                error,
                subject,
                body,
                extra={**sns_fields, "sns_error": error},
            )
        else:
            logger.info(
                "SNS failure alert sent: topic=%s message_id=%s subject=%r",
                self._topic_arn,
                response.get("MessageId"),
                subject,
                extra={**sns_fields, "sns_message_id": response.get("MessageId")},
            )

        return {"subject": subject, "message": body}

    def notify_json(self, classification: Classification, **kwargs: Any) -> str:
        """Machine-readable variant, for an EventBridge or ticketing integration."""
        return json.dumps(
            {
                "subject": build_subject(
                    classification, application=self._application, environment=self._environment
                ),
                **classification.to_dict(),
            },
            default=str,
        )
