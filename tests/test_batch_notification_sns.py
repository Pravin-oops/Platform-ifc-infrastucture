"""The batch-completion event against a real boto3 SNS client.

test_batch_notification.py fakes the client and checks the arguments; this file
drives the actual ``boto3.client("sns")`` path against a moto-backed SNS, with a
real SQS subscriber, so the assertions are about what a subscriber *receives*
rather than what we asked SNS to send: serialisation, message attributes,
subscription filtering and the exception types the notifier re-raises.

Still no AWS and no network - moto intercepts botocore. The whole module skips
if moto is not installed, so the default suite stays dependency-free.
"""

from __future__ import annotations

import json
import types

import pytest

moto = pytest.importorskip("moto", reason="moto is needed for the SNS integration tests")

import boto3
from botocore.exceptions import ClientError

from ifc_trigger_connector.utility.audit_utility import ReconciliationResult, RunCounters
from ifc_trigger_connector.utility.connector_config import NotificationSettings
from ifc_trigger_connector.utility.connector_runner import BatchResult
from ifc_trigger_connector.utility.trigger_batch_notifier import (
    TriggerBatchNotification,
    TriggerBatchNotifier,
)

REGION = "eu-west-2"


@pytest.fixture
def aws_credentials(monkeypatch):
    """Never let these tests reach a real account, whatever the environment holds."""
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SECURITY_TOKEN",
                 "AWS_SESSION_TOKEN"):
        monkeypatch.setenv(name, "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_CA_BUNDLE", raising=False)


@pytest.fixture
def topic(aws_credentials):
    """An SNS topic with an SQS subscriber, and a reader for what it received."""
    with moto.mock_aws():
        sns = boto3.client("sns", region_name=REGION)
        sqs = boto3.client("sqs", region_name=REGION)

        topic_arn = sns.create_topic(Name="ifc-trigger-connector-batch-complete")["TopicArn"]
        queue_url = sqs.create_queue(QueueName="tbb-subscriber")["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

        def received():
            messages = sqs.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=10
            ).get("Messages", [])
            return [json.loads(m["Body"]) for m in messages]

        yield types.SimpleNamespace(
            arn=topic_arn, sns=sns, sqs=sqs, queue_url=queue_url,
            queue_arn=queue_arn, received=received,
        )


def a_notification(**overrides) -> TriggerBatchNotification:
    fields = {
        "Trigger_Originating_BU": "UK-C",
        "No_Of_Messages_Produced": 412,
        "Trigger_Sub_Type": "NewHRCRelationship",
        "Topic_Name": "tc01_fncmtrgrbb_ifc_tbb_kyc_refresh",
        "Trigger_Batch_Start_Timestamp": "2026-07-01T02:00:00.000000000Z",
        "Trigger_Batch_End_Timestamp": "2026-07-01T02:04:37.000000000Z",
        "Event_Timestamp": "2026-07-01T02:04:38.512+00:00",
        "Correlation_Id": "run-7f3a91",
    }
    fields.update(overrides)
    return TriggerBatchNotification(**fields)


class TestAgainstARealSnsClient:
    def test_sns_accepts_the_publish(self, topic):
        # No client= injected: the notifier builds its own, as it does in ECS.
        response = TriggerBatchNotifier(sns_topic_arn=topic.arn).publish(a_notification())
        assert response["MessageId"]

    def test_the_subscriber_receives_the_eight_fields_intact(self, topic):
        notification = a_notification()
        TriggerBatchNotifier(sns_topic_arn=topic.arn).publish(notification)

        envelopes = topic.received()
        assert len(envelopes) == 1
        assert json.loads(envelopes[0]["Message"]) == notification.to_dict()

    def test_the_produced_count_stays_a_number_on_the_wire(self, topic):
        """A consumer reading it as a number must not be handed a string."""
        TriggerBatchNotifier(sns_topic_arn=topic.arn).publish(a_notification())
        body = json.loads(topic.received()[0]["Message"])
        assert body["No_Of_Messages_Produced"] == 412
        assert isinstance(body["No_Of_Messages_Produced"], int)

    def test_the_subject_reaches_the_subscriber(self, topic):
        TriggerBatchNotifier(sns_topic_arn=topic.arn).publish(a_notification())
        assert topic.received()[0]["Subject"] == "TBB Batch Complete - NewHRCRelationship"

    def test_the_message_attributes_reach_the_subscriber(self, topic):
        TriggerBatchNotifier(sns_topic_arn=topic.arn).publish(a_notification())
        attributes = topic.received()[0]["MessageAttributes"]
        assert attributes["trigger_sub_type"]["Value"] == "NewHRCRelationship"
        assert attributes["topic_name"]["Value"] == "tc01_fncmtrgrbb_ifc_tbb_kyc_refresh"
        assert attributes["correlation_id"]["Value"] == "run-7f3a91"

    def test_a_subscription_can_filter_on_the_trigger_sub_type(self, topic):
        """The point of the attributes: a per-trigger subscriber gets only its own."""
        queue_url = topic.sqs.create_queue(QueueName="trigger-9-only")["QueueUrl"]
        queue_arn = topic.sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        subscription = topic.sns.subscribe(
            TopicArn=topic.arn, Protocol="sqs", Endpoint=queue_arn
        )["SubscriptionArn"]
        topic.sns.set_subscription_attributes(
            SubscriptionArn=subscription,
            AttributeName="FilterPolicy",
            AttributeValue=json.dumps({"trigger_sub_type": ["AccountInactivity"]}),
        )

        def delivered():
            return topic.sqs.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=10
            ).get("Messages", [])

        notifier = TriggerBatchNotifier(sns_topic_arn=topic.arn)
        notifier.publish(a_notification(Trigger_Sub_Type="NewHRCRelationship"))
        assert delivered() == []

        notifier.publish(a_notification(Trigger_Sub_Type="AccountInactivity"))
        assert len(delivered()) == 1

    def test_a_missing_topic_raises_a_client_error(self, topic):
        """Caught, logged and re-raised: NotFoundException is a ClientError."""
        absent = "arn:aws:sns:eu-west-2:123456789012:no-such-topic"
        with pytest.raises(ClientError):
            TriggerBatchNotifier(sns_topic_arn=absent).publish(a_notification())

    def test_no_topic_configured_makes_no_aws_call(self, topic):
        payload = TriggerBatchNotifier(sns_topic_arn=None).publish(a_notification())
        assert payload == a_notification().to_dict()
        assert topic.received() == []


# ---------------------------------------------------------------------------
# The entry point, through the real notifier onto a real topic
# ---------------------------------------------------------------------------


def a_result(**overrides) -> BatchResult:
    counters = overrides.pop(
        "counters", RunCounters(records_parsed=412, published=412, acked=412)
    )
    fields = {
        "counters": counters,
        "reconciliation": ReconciliationResult(
            balanced=True, expected=412, accounted=412, findings=[]
        ),
        "delivery": {},
        "outcome": "SUCCESS",
        "exit_code": 0,
        "batch_start_timestamp": "2026-07-01T02:00:00.000000000Z",
        "batch_end_timestamp": "2026-07-01T02:04:37.000000000Z",
        "published_trigger_subtype": "NewHRCRelationship",
    }
    fields.update(overrides)
    return BatchResult(**fields)


def a_settings(topic_arn):
    return types.SimpleNamespace(
        notifications=NotificationSettings(
            batch_sns_topic_arn=topic_arn, trigger_originating_bu="UK-C"
        ),
        kafka=types.SimpleNamespace(topic="tc01_fncmtrgrbb_ifc_tbb_kyc_refresh"),
    )


class TestTheEcsPathReachesTheTopic:
    def test_a_clean_batch_lands_on_the_topic(self, topic, main_ecs_script):
        runner = types.SimpleNamespace(last_result=a_result(), run_id="run-ecs-01")
        main_ecs_script._publish_batch_notification(a_settings(topic.arn), runner)

        envelopes = topic.received()
        assert len(envelopes) == 1
        body = json.loads(envelopes[0]["Message"])
        assert body["No_Of_Messages_Produced"] == 412  # acked, not attempted
        assert body["Trigger_Sub_Type"] == "NewHRCRelationship"
        assert body["Topic_Name"] == "tc01_fncmtrgrbb_ifc_tbb_kyc_refresh"
        assert body["Correlation_Id"] == "run-ecs-01"
        assert body["Trigger_Originating_BU"] == "UK-C"
        # Raised now, not the end of the batch window.
        assert body["Event_Timestamp"] != body["Trigger_Batch_End_Timestamp"]

    @pytest.mark.parametrize(
        "reason, result",
        [
            ("the run did not succeed", a_result(outcome="PARTIAL")),
            (
                "the numbers do not add up",
                a_result(
                    reconciliation=ReconciliationResult(
                        balanced=False, expected=412, accounted=400, findings=["short"]
                    )
                ),
            ),
            (
                "a message failed delivery",
                a_result(counters=RunCounters(published=412, acked=411, delivery_failed=1)),
            ),
            (
                "a message never left the queue",
                a_result(counters=RunCounters(published=412, acked=411, unflushed=1)),
            ),
            ("nothing was delivered", a_result(counters=RunCounters())),
            ("there was no batch at all", None),
        ],
    )
    def test_an_incomplete_batch_puts_nothing_on_the_topic(
        self, topic, main_ecs_script, reason, result
    ):
        runner = types.SimpleNamespace(last_result=result, run_id="run-ecs-01")
        main_ecs_script._publish_batch_notification(a_settings(topic.arn), runner)
        assert topic.received() == [], reason
