"""Sequence numbers for the published envelope."""

from __future__ import annotations

import threading
from typing import Dict

from utility.tb_outcome_schema import MAX_SEQUENCE


class SequenceAllocator:
    """Numbers the batch, and counts occurrences of each CSID within it."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._position = 0
        self._seen: Dict[str, int] = {}

    def allocate(self, csid: str) -> int:
        """The next sequence number in the batch, starting at 1."""
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
