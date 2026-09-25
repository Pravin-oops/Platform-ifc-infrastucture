"""Per-customer occurrence numbers, and the trigger IDs they keep apart.

``sequenceNumber`` counts a customer's events within the batch: 1 for the first,
2 for a second event for the same CSID. It is part of the trigger ID from the
second onwards, which is what stops two events from one source being published
under a single identity.
"""

from __future__ import annotations

import pytest

from utility.connector_utility import load_schema_document
from utility.sequence_allocator import SequenceAllocator
from utility.tb_outcome_schema import (
    MAX_SEQUENCE,
    EnvelopeBuilder,
    TriggerEvent,
)


class TestTheAllocator:
    def test_each_customer_starts_at_one(self):
        allocator = SequenceAllocator()
        assert allocator.allocate("A") == 1
        assert allocator.allocate("B") == 1
        assert allocator.allocate("C") == 1

    def test_a_repeated_customer_counts_up(self):
        allocator = SequenceAllocator()
        assert [allocator.allocate("A") for _ in range(3)] == [1, 2, 3]

    def test_customers_are_counted_independently(self):
        allocator = SequenceAllocator()
        assert [allocator.allocate(c) for c in "ABAACB"] == [1, 1, 2, 3, 1, 2]
        assert allocator.customers == 3
        assert allocator.occurrences("A") == 3
        assert allocator.occurrences("B") == 2

    def test_an_unseen_customer_has_no_occurrences(self):
        assert SequenceAllocator().occurrences("nobody") == 0

    def test_it_wraps_rather_than_overflowing_the_avro_int(self):
        """sequenceNumber is an Avro int: a value past the ceiling would fail to
        encode rather than merely look odd."""
        allocator = SequenceAllocator()
        allocator._seen["A"] = MAX_SEQUENCE
        assert allocator.allocate("A") == 1

    def test_it_is_thread_safe(self):
        """The publish loop and its delivery callbacks run on different threads,
        so two events must never be handed the same number."""
        import threading

        allocator = SequenceAllocator()
        seen, lock = [], threading.Lock()

        def worker():
            for _ in range(200):
                value = allocator.allocate("A")
                with lock:
                    seen.append(value)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sorted(seen) == list(range(1, 801)), "a number was issued twice"


@pytest.fixture
def builder(app_root):
    import os

    def make():
        return EnvelopeBuilder(
            avro_schema=load_schema_document(os.path.join("utility", "schema.json")),
            sequence_allocator=SequenceAllocator(),
            business_month="2026-08",
        )

    return make


def an_event(csid, business_date="2026-08-31"):
    return TriggerEvent.from_dict(
        {
            "triggerSubType": "TRIGGER_8",
            "attributes": {
                "date_of_request": "2026-08-31 00:00:00",
                "counterparty_full_legal_entity_name": "Example Holdings Ltd",
                "counterparty_csid_sds": csid,
                "client_relationship_owner_name": "TOKENISED_NAME",
                "client_relationship_owner_brid": "BR123456",
                "business_date": business_date,
                "region": "EMEA",
            },
        }
    )


class TestTriggerIdUniqueness:
    def test_two_events_for_one_customer_get_different_ids(self, builder):
        """The reason the field is populated: without the occurrence in the key,
        these two hash to one identity and are published as the same event."""
        b = builder()
        first = b.build(an_event(9912345678)).record
        second = b.build(an_event(9912345678)).record

        assert first["sequenceNumber"] == 1
        assert second["sequenceNumber"] == 2
        assert first["triggerID"] != second["triggerID"]

    def test_many_events_for_one_customer_are_all_distinct(self, builder):
        b = builder()
        records = [b.build(an_event(9912345678)).record for _ in range(5)]

        assert [r["sequenceNumber"] for r in records] == [1, 2, 3, 4, 5]
        assert len({r["triggerID"] for r in records}) == 5

    def test_different_customers_each_start_at_one(self, builder):
        b = builder()
        first = b.build(an_event(9912345678)).record
        other = b.build(an_event(9912345679)).record

        assert first["sequenceNumber"] == other["sequenceNumber"] == 1
        assert first["triggerID"] != other["triggerID"]

    def test_the_first_occurrence_keeps_the_identity_it_always_had(self, builder):
        """The occurrence joins the key only from the second event, so an
        ordinary record - one event per customer - hashes exactly as before the
        field existed, and no consumer sees its identity move."""
        without = builder().business_key(an_event(9912345678), _definition())
        with_first = builder().business_key(an_event(9912345678), _definition(), occurrence=1)

        assert without == with_first
        assert "occurrence" not in without

    def test_a_repeat_adds_the_occurrence_to_the_key(self, builder):
        key = builder().business_key(an_event(9912345678), _definition(), occurrence=2)
        assert '"occurrence":"2"' in key

    def test_the_kafka_key_stays_the_customer(self, builder):
        """Partitioning is by CSID so one customer's events stay ordered on one
        partition - the occurrence separates identities, not partitions."""
        b = builder()
        first = b.build(an_event(9912345678))
        second = b.build(an_event(9912345678))

        assert first.kafka_key == second.kafka_key == "9912345678"


def _definition():
    from utility import trigger_definitions as definitions

    return definitions.resolve("TRIGGER_8")


class TestTheDefaultAllocator:
    """EnvelopeBuilder with no allocator must number the same way as one built
    explicitly. A second implementation behind that default is how the local dry
    run ended up numbering by file position instead of by customer.
    """

    def _records(self, b, csids):
        return [b.build(an_event(c)).record for c in csids]

    def test_the_default_numbers_per_customer(self, app_root):
        import os

        b = EnvelopeBuilder(
            avro_schema=load_schema_document(os.path.join("utility", "schema.json")),
            business_month="2026-08",
        )
        records = self._records(b, [9912345678, 9912345679, 9912345678])

        assert [r["sequenceNumber"] for r in records] == [1, 1, 2]

    def test_the_default_matches_an_explicit_allocator(self, builder, app_root):
        import os

        csids = [9912345678, 9912345679, 9912345678, 9912345680]
        explicit = self._records(builder(), csids)
        default = self._records(
            EnvelopeBuilder(
                avro_schema=load_schema_document(os.path.join("utility", "schema.json")),
                business_month="2026-08",
            ),
            csids,
        )

        assert [r["sequenceNumber"] for r in default] == [r["sequenceNumber"] for r in explicit]
        assert [r["triggerID"] for r in default] == [r["triggerID"] for r in explicit]
