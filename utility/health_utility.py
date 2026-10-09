from __future__ import annotations

import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

DEFAULT_LIVENESS_TIMEOUT_SECONDS = 300.0


class HealthState:
    def __init__(self, *, liveness_timeout: float = DEFAULT_LIVENESS_TIMEOUT_SECONDS):
        self._lock = threading.Lock()
        self._started_at = time.time()
        self._liveness_timeout = liveness_timeout
        self._last_heartbeat = time.time()
        self._draining = False
        self._detail: Dict[str, Any] = {}

    def heartbeat(self) -> None:
        with self._lock:
            self._last_heartbeat = time.time()

    def mark_draining(self) -> None:
        with self._lock:
            self._draining = True

    def update(self, **detail: Any) -> None:
        with self._lock:
            self._detail.update(detail)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            since_heartbeat = time.time() - self._last_heartbeat
            return {
                "draining": self._draining,
                "live": since_heartbeat < self._liveness_timeout,
                "uptime_seconds": round(time.time() - self._started_at, 1),
                "seconds_since_heartbeat": round(since_heartbeat, 1),
                **self._detail,
            }


class _Handler(BaseHTTPRequestHandler):
    state: HealthState
    metrics_provider: Callable[[], Dict[str, Any]]

    def _respond(self, code: int, body: Dict[str, Any]) -> None:
        payload = json.dumps(body, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802
        snapshot = self.state.snapshot()
        path = self.path.split("?", 1)[0].rstrip("/") or "/"

        if path in ("/health/live", "/healthz", "/"):
            self._respond(200 if snapshot["live"] else 503, snapshot)
        elif path == "/metrics":
            self._respond(200, {"health": snapshot, "metrics": self.metrics_provider()})
        else:
            self._respond(404, {"error": "not found", "path": path})

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        logger.debug("health probe: " + format, *args)


class HealthServer:
    def __init__(
        self,
        state: HealthState,
        *,
        host: str = "0.0.0.0",
        port: int = 8080,
        metrics_provider: Optional[Callable[[], Dict[str, Any]]] = None,
    ):
        self._state = state
        self._host = host
        self._port = port
        self._metrics_provider: Callable[[], Dict[str, Any]] = (
            metrics_provider if metrics_provider is not None else lambda: {}
        )
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> "HealthServer":
        handler = type(
            "BoundHealthHandler",
            (_Handler,),
            {"state": self._state, "metrics_provider": staticmethod(self._metrics_provider)},
        )

        try:
            self._server = ThreadingHTTPServer((self._host, self._port), handler)
        except OSError as exc:
            logger.error(
                "Health server could not bind; continuing without health endpoints",
                extra={"host": self._host, "port": self._port, "error": str(exc)},
            )
            return self

        self._thread = threading.Thread(target=self._server.serve_forever, name="health", daemon=True)
        self._thread.start()
        logger.debug("Health server listening", extra={"host": self._host, "port": self._port})
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            logger.debug("Health server stopped")
