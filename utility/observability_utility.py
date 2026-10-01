"""Structured logging and CloudWatch metrics for an ECS-hosted connector.

Two things matter here that do not matter in a Lambda:

* Logs are the only forensic record once the task is gone, so every line is
  JSON with a stable ``run_id`` and, where relevant, ``trigger_id``. CloudWatch
  Logs Insights can then answer "what happened to trigger X" directly.
* Metrics are emitted as embedded metric format (EMF) on stdout rather than via
  ``PutMetricData``. No API call means no extra failure mode inside the very
  path that reports failures, and CloudWatch still gets real metrics to alarm on.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from typing import Any, Dict, Optional

EMF_NAMESPACE = "IFC/TriggerConnector"

_context: Dict[str, Any] = {}
_context_lock = threading.Lock()


def set_log_context(**fields: Any) -> None:
    """Fields merged into every subsequent log line (run_id, batch_id, ...)."""
    with _context_lock:
        _context.update({k: v for k, v in fields.items() if v is not None})


def clear_log_context(*names: str) -> None:
    with _context_lock:
        for name in names:
            _context.pop(name, None)


class JsonFormatter(logging.Formatter):
    """One JSON object per line, with the ambient context merged in."""

    _RESERVED = {
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "module", "msecs",
        "message", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "thread", "threadName", "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        with _context_lock:
            payload.update(_context)

        for key, value in record.__dict__.items():
            if key not in self._RESERVED and not key.startswith("_"):
                payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())

    # librdkafka and botocore are chatty at DEBUG and drown the run narrative.
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


class Metrics:
    """Counters and timers for one connector process.

    Counters are running totals, and ``emit`` writes the current totals each
    time it is called - at every progress report and again at the end of the
    run. CloudWatch alarms on them must therefore use the ``Maximum``
    statistic: ``Sum`` adds the same records up once per emit.

    Thread-safe: the delivery-report callback fires on the librdkafka poll
    thread while the main loop is still producing.
    """

    def __init__(self, *, namespace: str = EMF_NAMESPACE, dimensions: Optional[Dict[str, str]] = None):
        self._namespace = namespace
        self._dimensions = dimensions or {}
        self._lock = threading.Lock()
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._emf_enabled = os.environ.get("IFC_EMF_ENABLED", "true").lower() != "false"

    def incr(self, name: str, value: float = 1.0) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0.0) + value

    def gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def get(self, name: str) -> float:
        with self._lock:
            return self._counters.get(name, self._gauges.get(name, 0.0))

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            return {**self._counters, **self._gauges}

    def emit(self, extra: Optional[Dict[str, Any]] = None) -> None:
        """Write one EMF record so CloudWatch ingests the current values."""
        if not self._emf_enabled:
            return

        values = self.snapshot()
        if not values:
            return

        document: Dict[str, Any] = {
            "_aws": {
                "Timestamp": int(time.time() * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": self._namespace,
                        "Dimensions": [list(self._dimensions.keys())] if self._dimensions else [[]],
                        "Metrics": [{"Name": name} for name in values],
                    }
                ],
            },
            **self._dimensions,
            **values,
        }
        if extra:
            document.update(extra)

        # Straight to stdout: the CloudWatch agent parses EMF out of the log stream.
        print(json.dumps(document, default=str), flush=True)


def process_rss_mb() -> Optional[float]:
    """Resident set size in MiB, read from cgroup v2/v1 then /proc.

    Used to publish a memory gauge so the 'Producer Out Of Memory' scenario is
    visible as a trend before the task is killed with exit code 137.
    """
    for path in ("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory/memory.usage_in_bytes"):
        try:
            with open(path, "r", encoding="utf-8") as handle:
                return int(handle.read().strip()) / (1024 * 1024)
        except (OSError, ValueError):
            continue

    try:
        with open("/proc/self/statm", "r", encoding="utf-8") as handle:
            pages = int(handle.read().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)
    except (OSError, ValueError, IndexError, AttributeError):
        return None


def memory_limit_mb() -> Optional[float]:
    """Container memory limit in MiB, so usage can be reported as a ratio."""
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            with open(path, "r", encoding="utf-8") as handle:
                raw = handle.read().strip()
            if raw == "max":
                return None
            limit = int(raw)
            # cgroup v1 reports an absurd sentinel when unlimited.
            return None if limit > (1 << 60) else limit / (1024 * 1024)
        except (OSError, ValueError):
            continue
    return None
