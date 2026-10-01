"""Sequence numbers for the published envelope.

``sequenceNumber`` is the record's position in the batch being published:
1, 2, 3... across every customer, the way the Trigger Backbone's reference
producer numbers its batch. It is also the last segment of the trigger ID, so
it must never repeat within a run - two records with one number would share a
trigger ID and a Kafka key.

The allocator also counts each customer's occurrences. That count is not
published; it goes into the business key, which is what tells two events for
the same customer apart.

It is allocated in process, for the life of the run. One monthly batch is one
file read by one run, and the file is read in order, so a re-run of the same
file numbers the same events the same way.

Numbers wrap rather than overflow: ``sequenceNumber`` is an Avro ``int``, so a
value past ``MAX_SEQUENCE`` would fail encoding rather than merely look odd.
"""

from __future__ import annotations

import threading
from typing import Dict

from utility.tb_outcome_schema import MAX_SEQUENCE


class SequenceAllocator:
    """Numbers the batch, and counts occurrences of each CSID within it.

    Thread-safe, because the publish loop and its delivery callbacks run on
    different threads.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._position = 0
        self._seen: Dict[str, int] = {}

    def allocate(self, csid: str) -> int:
        """The next sequence number in the batch, starting at 1.

        Also advances this customer's occurrence count.
        """
        with self._lock:
            self._position += 1
            if self._position > MAX_SEQUENCE:
                self._position = 1
            self._seen[csid] = self._seen.get(csid, 0) + 1
            return self._position

    def occurrences(self, csid: str) -> int:
        """How many times this customer has been numbered so far."""
        with self._lock:
            return self._seen.get(csid, 0)

    @property
    def customers(self) -> int:
        """How many distinct customers have been numbered."""
        with self._lock:
            return len(self._seen)
