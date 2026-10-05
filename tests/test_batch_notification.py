"""The Trigger Backbone batch-completion event.

Unlike the failure alert, this is a *successful business event*: TBB starts
downstream processing from it. So the tests here are mostly about when it must
NOT be sent - a half-delivered batch that still announced itself would start
downstream work on an incomplete topic.
"""

from __future__ import annotations

import json
import types

import pytest

from utility.audit_utility import ReconciliationResult, RunCounters
from utility.connector_config import NotificationSettings
from utility.connector_runner import BatchResult
from utility.trigger_batch_notifier import (
    TriggerBatchNotification,
    TriggerBatchNotifier,
)


class FakeSNS:
    def __init__(self):
        self.calls = []

    def publish(self, **kwargs):
        self.calls.append(kwargs)
        return {"MessageId": "message-1"}


def a_notification(**overrides) -> TriggerBatchNotification:
    fields = {
        "Trigger_Originating_BU": "UK-C",
        "No_Of_Messages_Produced": 3,
        "Trigger_Sub_Type": "NewHRCRelationship",
        "Topic_Name": "ifc_tbb_kyc_refresh",
        "Trigger_Batch_Start_Timestamp": "2026-07-01T02:00:00.000000000Z",
        "Trigger_Batch_End_Timestamp": "2026-07-01T02:00:09.000000000Z",
        "Event_Timestamp": "2026-07-01T02:00:10.123+00:00",
        "Correlation_Id": "run-abc",
    }
    fields.update(overrides)
    return TriggerBatchNotification(**fields)


class TestTriggerBatchNotifier:
    def test_the_message_is_the_eight_contract_fields(self):
        sns = FakeSNS()
        TriggerBatchNotifier(sns_topic_arn="arn:aws:sns:eu-west-2:1:tbb", client=sns).publish(
            a_notification()
        )

        message = json.loads(sns.calls[0]["Message"])
        assert message == {
            "Trigger_Originating_BU": "UK-C",
            "No_Of_Messages_Produced": 3,
            "Trigger_Sub_Type": "NewHRCRelationship",
            "Topic_Name": "ifc_tbb_kyc_refresh",
            "Trigger_Batch_Start_Timestamp": "2026-07-01T02:00:00.000000000Z",
            "Trigger_Batch_End_Timestamp": "2026-07-01T02:00:09.000000000Z",
            "Event_Timestamp": "2026-07-01T02:00:10.123+00:00",
            "Correlation_Id": "run-abc",
        }

    def test_the_subject_names_the_trigger_so_a_subscriber_can_route(self):
        sns = FakeSNS()
        TriggerBatchNotifier(sns_topic_arn="arn:tbb", client=sns).publish(a_notification())
        assert sns.calls[0]["Subject"] == "TBB Batch Complete - NewHRCRelationship"

    def test_message_attributes_allow_a_subscription_filter(self):
        sns = FakeSNS()
        TriggerBatchNotifier(sns_topic_arn="arn:tbb", client=sns).publish(a_notification())

        attributes = sns.calls[0]["MessageAttributes"]
        assert attributes["trigger_sub_type"]["StringValue"] == "NewHRCRelationship"
        assert attributes["topic_name"]["StringValue"] == "ifc_tbb_kyc_refresh"
        assert attributes["correlation_id"]["StringValue"] == "run-abc"

    def test_without_a_topic_it_logs_instead_of_calling_sns(self):
        """The local DEV path: no AWS, no network, still the same payload."""
        sns = FakeSNS()
        payload = TriggerBatchNotifier(sns_topic_arn=None, client=sns).publish(a_notification())

        assert sns.calls == []
        assert payload["No_Of_Messages_Produced"] == 3

    def test_a_publish_failure_is_raised_not_swallowed(self):
        from botocore.exceptions import BotoCoreError

        class BrokenSNS:
            def publish(self, **kwargs):
                raise BotoCoreError()

        with pytest.raises(BotoCoreError):
            TriggerBatchNotifier(sns_topic_arn="arn:tbb", client=BrokenSNS()).publish(
                a_notification()
            )

    def test_a_sent_message_is_logged_in_full_with_its_message_id(self, caplog):
        caplog.set_level("INFO")
        TriggerBatchNotifier(sns_topic_arn="arn:tbb", client=FakeSNS()).publish(a_notification())

        sent = [r for r in caplog.records if "sent:" in r.getMessage()]
        assert len(sent) == 1
        assert "message_id=message-1" in sent[0].getMessage()
        assert '"Correlation_Id": "run-abc"' in sent[0].getMessage()
        assert sent[0].sns_topic_arn == "arn:tbb"

    def test_a_failed_send_logs_the_message_and_the_aws_error(self, caplog):
        from botocore.exceptions import ClientError

        class DeniedSNS:
            def publish(self, **kwargs):
                raise ClientError(
                    {"Error": {"Code": "AuthorizationError", "Message": "not authorized"}},
                    "Publish",
                )

        with pytest.raises(ClientError):
            TriggerBatchNotifier(sns_topic_arn="arn:tbb", client=DeniedSNS()).publish(
                a_notification()
            )

        failed = [r for r in caplog.records if "FAILED" in r.getMessage()]
        assert len(failed) == 1
        assert failed[0].levelname == "ERROR"
        assert "AuthorizationError: not authorized" in failed[0].getMessage()
        assert '"No_Of_Messages_Produced": 3' in failed[0].getMessage()


# ---------------------------------------------------------------------------
# The entry-point guard: which batches are allowed to announce themselves
# ---------------------------------------------------------------------------


def a_result(**overrides) -> BatchResult:
    counters = overrides.pop("counters", RunCounters(records_parsed=3, published=3, acked=3))
    fields = {
        "counters": counters,
        "reconciliation": ReconciliationResult(
            balanced=True, expected=3, accounted=3, findings=[]
        ),
        "delivery": {},
        "outcome": "SUCCESS",
        "exit_code": 0,
        "batch_start_timestamp": "2026-07-01T02:00:00.000000000Z",
        "batch_end_timestamp": "2026-07-01T02:00:09.000000000Z",
        "published_trigger_subtype": "NewHRCRelationship",
    }
    fields.update(overrides)
    return BatchResult(**fields)


def a_settings(**overrides) -> types.SimpleNamespace:
    notifications = NotificationSettings(
        batch_sns_topic_arn="arn:aws:sns:eu-west-2:1:tbb", **overrides
    )
    return types.SimpleNamespace(
        notifications=notifications,
        kafka=types.SimpleNamespace(topic="ifc_tbb_kyc_refresh"),
    )


@pytest.fixture
def sent(main_ecs_script, monkeypatch):
    """Captures what _publish_batch_notification would put on the topic."""
    captured = []

    class Capturing(TriggerBatchNotifier):
        def publish(self, notification):
            captured.append(notification)
            return {}

    monkeypatch.setattr(main_ecs_script, "TriggerBatchNotifier", Capturing)
    return captured


class TestPublishBatchNotification:
    def test_a_clean_batch_is_announced(self, main_ecs_script, sent):
        runner = types.SimpleNamespace(last_result=a_result(), run_id="run-abc")
        main_ecs_script._publish_batch_notification(a_settings(), runner)

        assert len(sent) == 1
        assert sent[0].No_Of_Messages_Produced == 3
        assert sent[0].Trigger_Sub_Type == "NewHRCRelationship"
        assert sent[0].Topic_Name == "ifc_tbb_kyc_refresh"
        assert sent[0].Correlation_Id == "run-abc"
        assert sent[0].Trigger_Originating_BU == "UK-C"

    def test_the_event_timestamp_is_when_the_event_was_raised(self, main_ecs_script, sent):
        runner = types.SimpleNamespace(last_result=a_result(), run_id="run-abc")
        main_ecs_script._publish_batch_notification(a_settings(), runner)

        # Distinct from the batch window, which is when the records were posted.
        assert sent[0].Event_Timestamp > sent[0].Trigger_Batch_End_Timestamp[:10]
        assert sent[0].Trigger_Batch_Start_Timestamp == "2026-07-01T02:00:00.000000000Z"

    @pytest.mark.parametrize(
        "reason, result",
        [
            ("the run did not succeed", a_result(outcome="FAILED")),
            (
                "the numbers do not add up",
                a_result(
                    outcome="RECONCILIATION_FAILED",
                    reconciliation=ReconciliationResult(
                        balanced=False, expected=3, accounted=2, findings=["short"]
                    )
                ),
            ),
            ("nothing was delivered", a_result(counters=RunCounters())),
            (
                "a message failed delivery",
                a_result(outcome="RECONCILIATION_FAILED", counters=RunCounters(published=3, acked=2, delivery_failed=1)),
            ),
            (
                "a message never left the queue",
                a_result(outcome="RECONCILIATION_FAILED", counters=RunCounters(published=3, acked=2, unflushed=1)),
            ),
        ],
    )
    def test_an_incomplete_batch_is_not_announced(self, main_ecs_script, sent, reason, result):
        """TBB would start downstream work on a topic that is missing records."""
        runner = types.SimpleNamespace(last_result=result, run_id="run-abc")
        main_ecs_script._publish_batch_notification(a_settings(), runner)
        assert sent == [], reason

    def test_the_outcome_is_returned_for_the_run_summary(self, main_ecs_script, sent):
        clean = types.SimpleNamespace(last_result=a_result(), run_id="run-abc")
        failed = types.SimpleNamespace(last_result=a_result(outcome="FAILED"), run_id="run-abc")

        assert main_ecs_script._publish_batch_notification(a_settings(), clean)["status"] == "SENT"
        assert main_ecs_script._publish_batch_notification(a_settings(), failed) == {
            "status": "SKIPPED",
            "reason": "run outcome is FAILED, not SUCCESS",
        }

    def test_a_skipped_notification_logs_why(self, main_ecs_script, sent, caplog):
        runner = types.SimpleNamespace(last_result=a_result(outcome="FAILED"), run_id="run-abc")
        main_ecs_script._publish_batch_notification(a_settings(), runner)

        skipped = [r for r in caplog.records if "skipped" in r.getMessage()]
        assert len(skipped) == 1
        assert skipped[0].sns_skip_reason == "run outcome is FAILED, not SUCCESS"

    def test_nothing_is_sent_when_the_feature_is_off(self, main_ecs_script, sent):
        runner = types.SimpleNamespace(last_result=a_result(), run_id="run-abc")
        main_ecs_script._publish_batch_notification(
            a_settings(batch_notifications_enabled=False), runner
        )
        assert sent == []

    def test_nothing_is_sent_when_no_topic_is_configured(self, main_ecs_script, sent):
        settings = a_settings()
        settings.notifications.batch_sns_topic_arn = None
        runner = types.SimpleNamespace(last_result=a_result(), run_id="run-abc")
        main_ecs_script._publish_batch_notification(settings, runner)
        assert sent == []

    def test_a_run_that_never_produced_a_batch_is_not_announced(self, main_ecs_script, sent):
        """A start-up failure leaves last_result unset."""
        runner = types.SimpleNamespace(last_result=None, run_id="run-abc")
        main_ecs_script._publish_batch_notification(a_settings(), runner)
        assert sent == []


class TestNotificationSettings:
    def test_the_defaults_are_safe_without_a_topic(self):
        settings = NotificationSettings()
        assert settings.batch_sns_topic_arn is None
        assert settings.trigger_originating_bu == "UK-C"
        assert settings.batch_notifications_enabled is True

    def test_the_batch_topic_is_separate_from_the_failure_topic(self):
        settings = NotificationSettings(
            sns_topic_arn="arn:rtb", batch_sns_topic_arn="arn:tbb"
        )
        assert settings.sns_topic_arn != settings.batch_sns_topic_arn
