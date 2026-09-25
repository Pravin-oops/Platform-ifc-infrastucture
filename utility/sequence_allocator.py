"""Per-customer occurrence numbers for the published envelope.

``sequenceNumber`` counts how many times a customer has appeared in the batch
being published: the first event for a CSID is 1, a second event for the same
CSID is 2, and so on. It is what separates multiple events from one source, so
the identity of the second event differs from the first.

Numbering is per CSID rather than global, so the number is readable on its own -
"this is the second event for this customer this month" - instead of being a
position in the file that says nothing about the customer.

It is allocated in process, for the life of the run. That is all the contract
needs: one monthly batch is one file read by one run. De-duplication is the
consuming team's, so nothing here has to survive a restart - a re-run of the
same file numbers the same events the same way, because the file is read in
order.

Numbers wrap rather than overflow: ``sequenceNumber`` is an Avro ``int``, so a
value past ``MAX_SEQUENCE`` would fail encoding rather than merely look odd. A
customer would need more than two billion events in one batch to reach it.
"""

from __future__ import annotations

import threading
from typing import Dict

from ifc_trigger_connector.utility.tb_outcome_schema import MAX_SEQUENCE


class SequenceAllocator:
    """Counts occurrences of each CSID within one run.

    Every call advances that customer's count, because every call is a distinct
    event: two events for one customer must not be handed the same number, which
    is the whole reason the field is populated. Thread-safe, because the publish
    loop and its delivery callbacks run on different threads.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seen: Dict[str, int] = {}

    def allocate(self, csid: str) -> int:
        """The next occurrence number for this customer, starting at 1."""
        with self._lock:
            count = self._seen.get(csid, 0) + 1
            if count > MAX_SEQUENCE:
                count = 1
            self._seen[csid] = count
            return count

    def occurrences(self, csid: str) -> int:
        """How many times this customer has been numbered so far."""
        with self._lock:
            return self._seen.get(csid, 0)

    @property
    def customers(self) -> int:
        """How many distinct customers have been numbered."""
        with self._lock:
            return len(self._seen)
