"""The Trigger Backbone batch-completion SNS event.

Sent once a run has delivered its month, so TBB can start downstream processing.
The message is the notification's fields as JSON, keyed exactly as TBB expects.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from utility.failure_notifier import describe_aws_error

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class TriggerBatchNotification:
    Trigger_Originating_BU: str
    No_Of_Messages_Produced: int
    Trigger_Sub_Type: str
    Topic_Name: str
    Trigger_Batch_Start_Timestamp: str
    Trigger_Batch_End_Timestamp: str
    Event_Timestamp: str
    Correlation_Id: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class TriggerBatchNotifier:
    """
    Sends the Trigger Backbone batch-completion SNS event.

    Unlike failure_notifier.py, this represents a successful
    business event which TBB uses to begin downstream processing.
    """

    def __init__(
        self,
        *,
        sns_topic_arn: Optional[str] = None,
        client: Any = None,
    ):
        self._sns_topic_arn = sns_topic_arn
        self._client = client

    def _sns(self) -> Any:
        if self._client is None:
            self._client = boto3.client("sns")
        return self._client

    def publish(
        self,
        notification: TriggerBatchNotification,
    ) -> Dict[str, Any]:

        payload = notification.to_dict()
        subject = f"TBB Batch Complete - {notification.Trigger_Sub_Type}"
        message = json.dumps(payload)
        attributes = {
            "trigger_sub_type": notification.Trigger_Sub_Type,
            "topic_name": notification.Topic_Name,
            "correlation_id": notification.Correlation_Id,
        }

        # The whole message goes into the line itself, not only into structured
        # fields, so the ECS console and a plain CloudWatch search both show
        # exactly what TBB was (or would have been) sent.
        sns_fields = {
            "sns_kind": "batch_complete",
            "sns_topic_arn": self._sns_topic_arn,
            "sns_subject": subject,
            "sns_message": payload,
            "sns_message_attributes": attributes,
        }

        #
        # Local DEV mode
        #
        if not self._sns_topic_arn:
            logger.info(
                "SNS batch notification NOT sent (no topic configured); "
                "subject=%r message=%s",
                subject,
                message,
                extra=sns_fields,
            )
            return payload

        logger.debug(
            "SNS batch notification sending: topic=%s subject=%r message=%s",
            self._sns_topic_arn,
            subject,
            message,
            extra=sns_fields,
        )

        try:
            response = self._sns().publish(
                TopicArn=self._sns_topic_arn,
                Subject=subject,
                Message=message,
                MessageAttributes={
                    name: {"DataType": "String", "StringValue": value}
                    for name, value in attributes.items()
                },
            )

        except (ClientError, BotoCoreError) as exc:
            logger.exception(
                "SNS batch notification FAILED to send: topic=%s error=%s "
                "subject=%r message=%s",
                self._sns_topic_arn,
                describe_aws_error(exc),
                subject,
                message,
                extra={**sns_fields, "sns_error": describe_aws_error(exc)},
            )
            raise

        logger.info(
            "SNS batch notification sent: topic=%s message_id=%s subject=%r message=%s",
            self._sns_topic_arn,
            response.get("MessageId"),
            subject,
            message,
            extra={**sns_fields, "sns_message_id": response.get("MessageId")},
        )

        return response
