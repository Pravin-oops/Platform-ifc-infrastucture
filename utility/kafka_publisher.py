"""The Kafka publishing surface.

Uses the plain ``Producer`` rather than ``SerializingProducer`` because records
are serialised upstream (see ``serializers``), so the connector can size-check
and audit the exact bytes it is about to send.

Three behaviours here exist specifically because this runs as a container rather
than as a Lambda:

* ``BufferError`` from ``produce()`` is treated as back-pressure, not as an
  error. A Lambda with a small fixed batch never hits it; a resident task
  draining a large prefix hits it constantly, and growing the queue instead is
  how a container ends up OOM-killed.
* Delivery reports carry the partition and offset of every accepted message, so
  the run manifest can state exactly what was written - the evidence the
  reconciliation control needs.
* Every delivery failure is classified as it arrives, so a run that dies later
  can still report which scenario dominated.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from utility import failure_catalog as catalog
from utility.error_classifier import Classification, PublishError, classify
from utility.resilience_utility import ShutdownSignal

logger = logging.getLogger(__name__)


@dataclass
class DeliveryStats:
    """Aggregated delivery outcomes. Mutated from the librdkafka poll thread."""

    success: int = 0
    failure: int = 0
    by_scenario: Dict[str, int] = field(default_factory=dict)
    #: topic -> partition -> (min offset, max offset)
    offsets: Dict[str, Dict[int, Tuple[int, int]]] = field(default_factory=dict)
    latency_ms_total: float = 0.0
    latency_ms_max: float = 0.0
    first_failure: Optional[Classification] = None
    #: Trigger IDs the broker actually acknowledged, and where it put them.
    #: Only these are marked published in the state store, so an unacknowledged
    #: message is retried by the next run rather than being silently dropped.
    acked: Dict[str, Tuple[Optional[int], Optional[int]]] = field(default_factory=dict)
    failed: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        acked = self.success or 1
        return {
            "acked": self.success,
            "failed": self.failure,
            "by_scenario": dict(self.by_scenario),
            "offsets": {
                topic: {str(p): {"min": lo, "max": hi} for p, (lo, hi) in partitions.items()}
                for topic, partitions in self.offsets.items()
            },
            "ack_latency_ms_avg": round(self.latency_ms_total / acked, 1) if self.success else 0.0,
            "ack_latency_ms_max": round(self.latency_ms_max, 1),
            "first_failure": self.first_failure.to_dict() if self.first_failure else None,
            # The full acked map can hold tens of thousands of ids; the manifest
            # carries the offset ranges instead, and a sample for spot checks.
            "failed_trigger_ids": self.failed[:50],
            "acked_trigger_id_sample": list(self.acked)[:10],
        }


class Publisher:
    def __init__(
        self,
        producer: Any,
        *,
        topic: str,
        metrics: Any,
        shutdown: Optional[ShutdownSignal] = None,
        poll_timeout: float = 0.5,
        max_backpressure_seconds: float = 60.0,
    ):
        self._producer = producer
        self._topic = topic
        self._metrics = metrics
        self._shutdown = shutdown
        self._poll_timeout = poll_timeout
        self._max_backpressure = max_backpressure_seconds

        self._lock = threading.Lock()
        self.stats = DeliveryStats()
        #: Send times, so ack latency is measured rather than guessed.
        self._sent_at: Dict[str, float] = {}

    # -- delivery callbacks ------------------------------------------------

    def _record_offset(self, topic: str, partition: int, offset: int) -> None:
        partitions = self.stats.offsets.setdefault(topic, {})
        current = partitions.get(partition)
        partitions[partition] = (
            offset if current is None else min(current[0], offset),
            offset if current is None else max(current[1], offset),
        )

    def _on_delivery(self, err: Any, msg: Any, *, trigger_id: str) -> None:
        # librdkafka can invoke the callback with no message on some errors,
        # so every use of msg is guarded. The try/except stays for the C
        # object itself, which can raise on attribute access after free.
        kafka_key = None
        if msg is not None:
            try:
                key = msg.key()
                kafka_key = key.decode("utf-8") if isinstance(key, bytes) else key
            except Exception:  # pragma: no cover - defensive around the C object
                pass

        with self._lock:
            if err is not None:
                self.stats.failure += 1
                self.stats.failed.append(trigger_id)

                classification = classify(
                    err,
                    operation="delivery_report",
                    topic=self._topic,
                    context={"kafka_key": kafka_key, "trigger_id": trigger_id},
                )
                key_name = classification.scenario.key
                self.stats.by_scenario[key_name] = self.stats.by_scenario.get(key_name, 0) + 1
                self.stats.first_failure = self.stats.first_failure or classification

                self._metrics.incr("MessagesFailed")
                logger.error(
                    "Delivery failed",
                    extra={
                        "kafka_key": kafka_key,
                        "trigger_id": trigger_id,
                        **classification.to_dict(),
                    },
                )
                return

            self.stats.success += 1
            self._metrics.incr("MessagesAcked")

            partition = offset = None
            if msg is not None:
                try:
                    partition, offset = msg.partition(), msg.offset()
                    self._record_offset(msg.topic(), partition, offset)
                except Exception:  # pragma: no cover
                    pass

            self.stats.acked[trigger_id] = (partition, offset)

            sent_at = self._sent_at.pop(trigger_id, None)
            if sent_at is not None:
                latency = (time.perf_counter() - sent_at) * 1000.0
                self.stats.latency_ms_total += latency
                self.stats.latency_ms_max = max(self.stats.latency_ms_max, latency)

    # -- producing ---------------------------------------------------------

    def _produce_with_backpressure(
        self,
        *,
        topic: str,
        key: str,
        value: bytes,
        callback: Callable[[Any, Any], None],
        headers: Optional[List[Tuple[str, bytes]]] = None,
    ) -> None:
        """Enqueue, waiting for the local queue to drain rather than growing it."""
        deadline = time.monotonic() + self._max_backpressure
        waits = 0

        while True:
            try:
                self._producer.produce(
                    topic=topic,
                    key=key.encode("utf-8"),
                    value=value,
                    headers=headers,
                    on_delivery=callback,
                )
                # Serve delivery callbacks without blocking the produce loop.
                self._producer.poll(0)
                if waits:
                    self._metrics.incr("BackpressureWaits", waits)
                return

            except BufferError:
                waits += 1
                if time.monotonic() > deadline:
                    raise PublishError(
                        f"Local producer queue full for over {self._max_backpressure:.0f}s; "
                        "the broker is not draining",
                        catalog.HIGH_PUBLISH_LATENCY,
                        context={"topic": topic, "queue_len": len(self._producer)},
                    )

                if self._shutdown is not None and self._shutdown.is_set:
                    raise PublishError(
                        "Shutdown requested while the producer queue was full",
                        catalog.CONTAINER_FAILURE,
                        context={"topic": topic},
                    )

                # poll() both drains the queue and serves callbacks.
                self._producer.poll(self._poll_timeout)

            except Exception as exc:
                classification = classify(exc, operation="produce", topic=topic)
                raise PublishError(
                    f"produce() rejected the message: {exc}",
                    classification.scenario,
                    context={"topic": topic},
                    cause=exc,
                ) from exc

    def publish(
        self,
        *,
        key: str,
        value: bytes,
        trigger_id: str,
        headers: Optional[List[Tuple[str, bytes]]] = None,
    ) -> None:
        with self._lock:
            self._sent_at[trigger_id] = time.perf_counter()

        self._produce_with_backpressure(
            topic=self._topic,
            key=key,
            value=value,
            callback=lambda err, msg: self._on_delivery(err, msg, trigger_id=trigger_id),
            headers=headers,
        )
        self._metrics.incr("MessagesProduced")
        logger.debug("Queued message", extra={"trigger_id": trigger_id, "topic": self._topic})


    def poll(self, timeout: float = 0) -> int:
        return self._producer.poll(timeout)

    @property
    def queue_depth(self) -> int:
        try:
            return len(self._producer)
        except TypeError:  # pragma: no cover - not all stubs implement __len__
            return 0

    def flush(self, timeout_seconds: float) -> int:
        """Block until the queue drains or ``timeout_seconds`` elapses.

        Returns the number of messages still queued: non-zero means delivery is
        *uncertain*, not failed, and the caller must treat the run as incomplete
        rather than successful.
        """
        logger.info(
            "Flushing producer", extra={"queue_depth": self.queue_depth, "timeout_seconds": timeout_seconds}
        )
        remaining = self._producer.flush(timeout_seconds)
        if remaining:
            logger.error(
                "Flush timed out with messages still queued; delivery is uncertain",
                extra={"remaining": remaining},
            )
        self._metrics.gauge("QueueDepthAfterFlush", remaining)
        return remaining
