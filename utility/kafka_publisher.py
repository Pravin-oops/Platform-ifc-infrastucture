"""The Kafka publishing surface."""

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
    #: Trigger IDs the broker acknowledged, with partition and offset.
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

    def _record_offset(self, topic: str, partition: int, offset: int) -> None:
        partitions = self.stats.offsets.setdefault(topic, {})
        current = partitions.get(partition)
        partitions[partition] = (
            offset if current is None else min(current[0], offset),
            offset if current is None else max(current[1], offset),
        )

    def _on_delivery(self, err: Any, msg: Any, *, trigger_id: str) -> None:
        # librdkafka may pass no message on some errors, and the C object can raise after free.
        kafka_key = None
        if msg is not None:
            try:
                key = msg.key()
                kafka_key = key.decode("utf-8") if isinstance(key, bytes) else key
            except Exception:  # pragma: no cover - defensive around the C object
                pass

        with self._lock:
            if err is not None:
                self._sent_at.pop(trigger_id, None)
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
                    "Message NOT published: topic=%s trigger_id=%s scenario=%s error=%s",
                    self._topic,
                    trigger_id,
                    key_name,
                    err,
                    extra={
                        "topic": self._topic,
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

            latency = None
            sent_at = self._sent_at.pop(trigger_id, None)
            if sent_at is not None:
                latency = (time.perf_counter() - sent_at) * 1000.0
                self.stats.latency_ms_total += latency
                self.stats.latency_ms_max = max(self.stats.latency_ms_max, latency)

        # Logged on the broker's acknowledgement, not on produce(): only an ack
        # means the record is on the topic.
        logger.debug(
            "Message published: topic=%s partition=%s offset=%s trigger_id=%s",
            self._topic,
            partition,
            offset,
            trigger_id,
            extra={
                "topic": self._topic,
                "partition": partition,
                "offset": offset,
                "trigger_id": trigger_id,
                "kafka_key": kafka_key,
                "ack_latency_ms": round(latency, 1) if latency is not None else None,
            },
        )

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

        try:
            self._produce_with_backpressure(
                topic=self._topic,
                key=key,
                value=value,
                callback=lambda err, msg: self._on_delivery(err, msg, trigger_id=trigger_id),
                headers=headers,
            )
        except PublishError:
            # Never queued, so no delivery report will ever clear the send time.
            with self._lock:
                self._sent_at.pop(trigger_id, None)
            raise
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
        """Block until the queue drains or ``timeout_seconds`` elapses."""
        logger.debug(
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
