from __future__ import annotations

import logging
import random
import signal
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ShutdownSignal:
    def __init__(self) -> None:
        self._event = threading.Event()
        self._reason: Optional[str] = None
        self._installed = False

    def install(self) -> "ShutdownSignal":
        if self._installed:
            return self

        def handler(signum: int, _frame: Any) -> None:
            name = signal.Signals(signum).name
            logger.warning("Received %s; draining", name, extra={"signal": name})
            self.set(f"signal:{name}")

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):
                logger.debug("Could not install handler for %s", sig)

        self._installed = True
        return self

    def set(self, reason: str = "requested") -> None:
        self._reason = self._reason or reason
        self._event.set()

    @property
    def is_set(self) -> bool:
        return self._event.is_set()

    @property
    def reason(self) -> Optional[str]:
        return self._reason

    def sleep(self, seconds: float) -> bool:
        return self._event.wait(timeout=max(0.0, seconds))


@dataclass
class BackoffPolicy:
    base_seconds: float = 1.0
    max_seconds: float = 60.0
    multiplier: float = 2.0
    jitter: bool = True

    def delay(self, attempt: int) -> float:
        raw = min(self.base_seconds * (self.multiplier ** max(0, attempt - 1)), self.max_seconds)
        return random.uniform(0.0, raw) if self.jitter else raw

    def delays(self, attempts: int) -> Iterator[float]:
        for attempt in range(1, attempts + 1):
            yield self.delay(attempt)


def retry(
    operation: Callable[[], T],
    *,
    attempts: int,
    policy: BackoffPolicy,
    retry_on: Callable[[BaseException], bool],
    shutdown: Optional[ShutdownSignal] = None,
    description: str = "operation",
) -> T:
    last_error: Optional[BaseException] = None

    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:  # noqa: BLE001
            last_error = exc

            if not retry_on(exc):
                raise

            if attempt >= attempts:
                logger.error(
                    "%s failed after %s attempts",
                    description,
                    attempts,
                    extra={"attempts": attempts, "error": str(exc)},
                )
                raise

            delay = policy.delay(attempt)
            logger.warning(
                "%s failed (attempt %s/%s); retrying in %.1fs",
                description,
                attempt,
                attempts,
                delay,
                extra={"attempt": attempt, "delay_seconds": round(delay, 2), "error": str(exc)},
            )

            if shutdown is not None and shutdown.sleep(delay):
                logger.warning("Shutdown during backoff; abandoning %s", description)
                raise
            elif shutdown is None:
                time.sleep(delay)

    raise last_error if last_error else RuntimeError(f"{description} failed with no error recorded")


class CircuitOpen(RuntimeError):
    def __init__(self, message: str, *, last_error: Optional[BaseException] = None):
        super().__init__(message)
        self.last_error = last_error


class CircuitBreaker:
    def __init__(self, *, threshold: int, reset_seconds: float, name: str = "publish"):
        self._threshold = threshold
        self._reset_seconds = reset_seconds
        self._name = name
        self._failures = 0
        self._opened_at: Optional[float] = None
        self._last_error: Optional[BaseException] = None
        self._lock = threading.Lock()

    @property
    def is_open(self) -> bool:
        with self._lock:
            if self._opened_at is None:
                return False
            if time.monotonic() - self._opened_at >= self._reset_seconds:
                logger.info("Circuit '%s' half-opening after cooldown", self._name)
                self._opened_at = None
                self._failures = 0
                return False
            return True

    @property
    def consecutive_failures(self) -> int:
        with self._lock:
            return self._failures

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._opened_at = None

    def record_failure(self, error: Optional[BaseException] = None) -> None:
        with self._lock:
            self._failures += 1
            self._last_error = error or self._last_error
            if self._failures >= self._threshold and self._opened_at is None:
                self._opened_at = time.monotonic()
                logger.error(
                    "Circuit '%s' opened after %s consecutive failures",
                    self._name,
                    self._failures,
                    extra={"consecutive_failures": self._failures},
                )

    def raise_if_open(self) -> None:
        if self.is_open:
            raise CircuitOpen(
                f"Circuit '{self._name}' is open after {self.consecutive_failures} consecutive failures",
                last_error=self._last_error,
            )
