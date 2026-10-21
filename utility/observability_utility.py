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
    with _context_lock:
        _context.update({k: v for k, v in fields.items() if v is not None})


def clear_log_context(*names: str) -> None:
    with _context_lock:
        for name in names:
            _context.pop(name, None)


class JsonFormatter(logging.Formatter):
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

    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


class Metrics:
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

        print(json.dumps(document, default=str), flush=True)


def process_rss_mb() -> Optional[float]:
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
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            with open(path, "r", encoding="utf-8") as handle:
                raw = handle.read().strip()
            if raw == "max":
                return None
            limit = int(raw)
            return None if limit > (1 << 60) else limit / (1024 * 1024)
        except (OSError, ValueError):
            continue
    return None
