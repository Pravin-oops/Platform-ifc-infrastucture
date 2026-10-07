"""Assembles the Kafka publishing stack and runs the readiness checks.

Everything that can fail before a record is read fails here, in the order the
POC execution plan prescribes: source readable, state store reachable, DNS, TCP,
BAM token, Schema Registry, cluster metadata. By the time ``build`` returns, the
only failures left are genuine runtime ones.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional

from utility import ca_certificate
from utility.auth_helper import BSPClient, TokenProvider
from utility.cyberark_ccp_fetch import CyberArkAuthenticator
from utility import failure_catalog as catalog
from utility.error_classifier import PreflightError
from utility.connector_utility import (
    load_schema_document,
    materialise_local,
    package_resource,
)
from utility import kafka_preflight as pf
from utility.kafka_publisher import Publisher
from utility.schema_registry_client import (
    SchemaRegistryClient,
    compare_schemas,
    value_subject,
)
from utility.kafka_serializers import AvroSerializer, SizeGuard, build_serializer
from utility.resilience_utility import BackoffPolicy, ShutdownSignal
from utility.connector_config import ConnectorSettings

logger = logging.getLogger(__name__)

#: librdkafka properties the connector insists on. The BSP client owns security;
#: these are the delivery-guarantee and memory-bound properties the failure
#: catalogue depends on, so they are applied over whatever BSP returns.
REQUIRED_PRODUCER_PROPERTIES: Dict[str, Any] = {
    # Exactly-once-per-partition on retry. Without it, the automatic retries
    # that make 'partition leader failure' self-healing create duplicates.
    "enable.idempotence": True,
    "acks": "all",
    # Bound the local queue: this is the OOM guard that turns a slow broker into
    # back-pressure instead of unbounded heap growth.
    "queue.buffering.max.messages": 20000,
    "queue.buffering.max.kbytes": 262144,
    # Fail a message rather than retry it forever, so a stuck partition surfaces.
    "message.timeout.ms": 300000,
    "compression.type": "gzip",
}


class _NoTokenProvider:
    """Stand-in for the local, non-BSP path, where nothing needs a bearer token."""

    seconds_remaining = 0.0

    def get(self, *, force_refresh: bool = False) -> str:
        raise PreflightError(
            "A Schema Registry token was requested, but no BSP client is configured",
            catalog.AUTHENTICATION_FAILURE,
        )


class _BrokerErrors:
    """``error_cb`` that keeps librdkafka's recent per-broker errors.

    A failed metadata request only says ``_TRANSPORT``; the reason each
    connection was dropped - a TLS handshake failure, a SASL rejection - comes
    through ``error_cb``, so it is kept to explain the failure. Any callback
    the BSP config set is still called.
    """

    def __init__(self, chained: Any = None, *, keep: int = 10):
        self._chained = chained if callable(chained) else None
        self._messages: Deque[str] = deque(maxlen=keep)

    def __call__(self, error: Any) -> None:
        message = str(error)
        logger.warning("librdkafka error: %s", message)
        if message not in self._messages:
            self._messages.append(message)
        if self._chained is not None:
            self._chained(error)

    @property
    def recent(self) -> List[str]:
        return list(self._messages)


class _LibrdkafkaTrace(logging.Handler):
    """Keeps librdkafka's most recent log lines, to show what led to a failure.

    confluent-kafka queues librdkafka's lines for the Python ``logger`` until
    ``poll()``; the failed metadata request polls, so the lines arrive then.
    """

    #: The lines that say why a connection did not get through.
    KEY_LINE = re.compile(
        r"SSL|SASL|OAUTH|AUTH|FAIL|ERROR|certificate|handshake|disconnect|closed|refused|denied|token|expired",
        re.IGNORECASE,
    )

    def __init__(self, keep: int = 400):
        super().__init__(logging.DEBUG)
        self.lines: Deque[str] = deque(maxlen=keep)

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(record.getMessage())

    #: A broker's name carries its protocol (sasl_ssl://host:port), which would
    #: make every line about that broker look like a SASL/SSL line.
    BROKER_NAME = re.compile(r"\b(?:sasl_ssl|sasl_plaintext|ssl|plaintext)://\S+", re.IGNORECASE)

    def key_lines(self, limit: int) -> List[str]:
        return [
            line
            for line in self.lines
            # INIT is librdkafka's start-up banner, listing its ssl/sasl features.
            if not line.startswith("INIT ") and self.KEY_LINE.search(self.BROKER_NAME.sub("", line))
        ][-limit:]


def _jwt_claims(token: str) -> Dict[str, Any]:
    """The JWT's identifying claims, unverified, without the signature or the token."""
    try:
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    except (IndexError, ValueError, TypeError):
        return {}
    return {key: claims.get(key) for key in ("sub", "iss", "aud", "exp") if key in claims}


def _observed_oauth_cb(callback: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Wrap the BSP client's ``oauth_cb`` to log what it gives librdkafka.

    The brokers get their token through this callback, not through the
    ``get_token`` call the Schema Registry uses, so a token the registry
    accepts says nothing about this path. confluent-kafka expects
    ``(token, expiry in epoch seconds[, principal, extensions])``; an expiry in
    milliseconds, an exception, or the wrong shape leaves SASL waiting and the
    metadata request failing as ``_TRANSPORT``. Never logs the token itself.
    """

    def observed(oauth_config: Any) -> Any:
        try:
            result = callback(oauth_config)
        except Exception as exc:
            logger.error("oauth_cb raised %s: %s; librdkafka has no token for SASL", type(exc).__name__, exc)
            raise

        parts = result if isinstance(result, (tuple, list)) else (result,)
        token = parts[0] if parts else None
        expiry = parts[1] if len(parts) > 1 else None
        expires_in = expiry - time.time() if isinstance(expiry, (int, float)) else None
        logger.info(
            "oauth_cb supplied a token: shape=%d-tuple token_type=%s expires_in_seconds=%s principal=%s",
            len(parts),
            type(token).__name__,
            round(expires_in) if expires_in is not None else None,
            parts[2] if len(parts) > 2 else None,
            extra={
                "oauth_token_claims": _jwt_claims(token) if isinstance(token, str) else {},
                "oauth_expiry_raw": expiry,
            },
        )
        if expires_in is not None and expires_in <= 0:
            logger.error(
                "oauth_cb returned a token that has already expired (expiry=%s); librdkafka "
                "rejects it and SASL cannot authenticate",
                expiry,
            )
        elif expires_in is not None and expires_in > 7 * 24 * 3600:
            logger.warning(
                "oauth_cb expiry %s is more than 7 days away; if it is in milliseconds, librdkafka "
                "accepts the token but never refreshes it before the real expiry",
                expiry,
            )
        elif token:
            observed.supplied.set()
        return result

    #: Set once the callback has handed librdkafka a usable token; the
    #: factory waits on it before the first broker request.
    observed.supplied = threading.Event()  # type: ignore[attr-defined]
    return observed


#: Producer properties safe to log: no credentials, tokens or callbacks.
_LOGGED_PROPERTIES = (
    "security.protocol",
    "sasl.mechanism",
    "ssl.ca.location",
    "ssl.endpoint.identification.algorithm",
    "request.timeout.ms",
    "connections.max.idle.ms",
    "metadata.max.age.ms",
)


@dataclass
class KafkaStack:
    publisher: Publisher
    serializer: AvroSerializer
    size_guard: SizeGuard
    token_provider: TokenProvider
    schema_id: int
    preflight: Dict[str, Any]
    producer: Any
    #: Where the schema id came from: the registry subject and version in SECURE
    #: mode, or ``{"mode": "DEV", ...}`` when it is pinned in config.
    schema_context: Dict[str, Any] = field(default_factory=dict)


class KafkaStackFactory:
    def __init__(
        self,
        settings: ConnectorSettings,
        *,
        metrics: Any,
        shutdown: ShutdownSignal,
        producer_factory: Any = None,
    ):
        self._settings = settings
        self._metrics = metrics
        self._shutdown = shutdown
        self._producer_factory = producer_factory
        self._report = pf.PreflightReport()
        self._broker_errors: Optional[_BrokerErrors] = None
        self._trace: Optional[_LibrdkafkaTrace] = None

    @property
    def report(self) -> pf.PreflightReport:
        return self._report

    # -- pieces ------------------------------------------------------------

    def _bsp_client(self) -> Optional[BSPClient]:
        """None means a direct broker connection with no BSP involvement."""
        settings = self._settings

        if not settings.kafka.bsp_config_path:
            logger.warning(
                "No BSP client config: connecting directly with kafka.overrides. "
                "This path has no BAM authentication and is for local development only."
            )
            return None

        if settings.cyberark is not None and settings.cyberark.enabled:
            CyberArkAuthenticator(settings.cyberark).export_to_environment(settings.cyberark.principal_realm)
        else:
            logger.warning(
                "CyberArk is not enabled; relying on BSP_USERNAME/BSP_PASSWORD already in the environment"
            )

        # package_resource anchors a relative path at the connector root, so it
        # does not depend on the working directory.
        return BSPClient(materialise_local(package_resource(settings.kafka.bsp_config_path)))

    def _producer_config(self, bsp: Optional[BSPClient]) -> Dict[str, Any]:
        overrides = dict(REQUIRED_PRODUCER_PROPERTIES)
        overrides["queue.buffering.max.messages"] = self._settings.kafka.local_queue_max_messages
        # message.max.bytes must not be smaller than the guard, or librdkafka
        # rejects records the guard would have allowed.
        overrides["message.max.bytes"] = max(
            self._settings.kafka.max_message_bytes + 1024, 1024 * 1024
        )
        overrides.update(self._settings.kafka.overrides)

        config = bsp.producer_config(overrides) if bsp is not None else overrides
        config["error_cb"] = self._broker_errors = _BrokerErrors(config.get("error_cb"))
        if callable(config.get("oauth_cb")):
            config["oauth_cb"] = _observed_oauth_cb(config["oauth_cb"])
        self._enable_debug(config)
        self._log_client_config(config)

        if not self._producer_factory:
            try:
                from confluent_kafka import Producer
            except ImportError as exc:
                raise PreflightError(
                    "confluent-kafka is not installed in the image",
                    catalog.CONTAINER_FAILURE,
                    cause=exc,
                ) from exc
            self._producer_factory = Producer

        return config

    def _enable_debug(self, config: Dict[str, Any]) -> None:
        """Route librdkafka's debug trace into the JSON logs when kafka.debug is set.

        Also honours a ``debug`` property that arrived through kafka.overrides
        or the BSP config, so the trace is never left on stderr unparsed.
        """
        debug = self._settings.kafka.debug or config.get("debug")
        if not debug:
            return

        rdkafka = logging.getLogger("librdkafka")
        # Its own level, so the trace appears even when the run logs at INFO.
        rdkafka.setLevel(logging.DEBUG)
        for handler in [h for h in rdkafka.handlers if isinstance(h, _LibrdkafkaTrace)]:
            rdkafka.removeHandler(handler)
        self._trace = _LibrdkafkaTrace()
        rdkafka.addHandler(self._trace)

        config["debug"] = debug
        config["log_level"] = 7
        config["logger"] = rdkafka
        logger.warning("librdkafka debug is on (%s); turn it off once diagnosed", debug)

    @staticmethod
    def _log_client_config(config: Dict[str, Any]) -> None:
        """What librdkafka will actually connect with, after the BSP client built it.

        The BSP client may supply the brokers and security settings itself, so
        the YAML alone does not say what is in effect.
        """
        brokers = pf.parse_bootstrap_servers(config.get("bootstrap.servers", ""))
        effective = {key: config.get(key) for key in _LOGGED_PROPERTIES}
        ca_location = effective.get("ssl.ca.location")
        logger.info(
            "Kafka client config: security.protocol=%s sasl.mechanism=%s ssl.ca.location=%s "
            "(exists=%s) brokers=%d ports=%s request.timeout.ms=%s connections.max.idle.ms=%s "
            "metadata.max.age.ms=%s",
            effective["security.protocol"],
            effective["sasl.mechanism"],
            ca_location,
            bool(ca_location) and os.path.exists(str(ca_location)),
            len(brokers),
            sorted({port for _host, port in brokers}),
            effective["request.timeout.ms"],
            effective["connections.max.idle.ms"],
            effective["metadata.max.age.ms"],
            extra={
                **{key.replace(".", "_"): value for key, value in effective.items()},
                "bootstrap_servers": [f"{host}:{port}" for host, port in brokers],
                "oauth_cb_set": callable(config.get("oauth_cb")),
            },
        )

    def _network_checks(self, config: Dict[str, Any]) -> None:
        settings = self._settings
        timeout = settings.resilience.preflight_timeout_seconds

        brokers = pf.parse_bootstrap_servers(config.get("bootstrap.servers", ""))
        if not brokers:
            raise PreflightError(
                "The BSP client config contains no bootstrap.servers",
                catalog.TOPIC_UNAVAILABLE,
                context={"bsp_config_path": settings.kafka.bsp_config_path},
            )

        pf.check_dns(self._report, brokers, label="kafka")
        pf.check_tcp(self._report, brokers, label="kafka", timeout=timeout, require_all=False)

        if settings.schema_registry.mode == "SECURE" and settings.schema_registry.urls:
            # Every node is checked and reported; one reachable node is enough,
            # since the lookup fails over to whichever answers.
            registry = [pf.parse_url_endpoint(url) for url in settings.schema_registry.urls]
            single = len(registry) == 1
            pf.check_dns(self._report, registry, label="schema_registry", blocking=single)
            pf.check_tcp(
                self._report, registry, label="schema_registry", timeout=timeout, require_all=single
            )

    def _resolve_schema(self, tokens: TokenProvider) -> tuple[Dict[str, Any], int, Dict[str, Any]]:
        settings = self._settings
        local_schema = load_schema_document(settings.schema_registry.schema_path)

        if settings.schema_registry.mode == "DEV":
            schema_id = settings.schema_registry.schema_id
            if schema_id is None:
                # Unframed Avro is undecodable by KafkaAvroDeserializer, so refuse
                # to start rather than publish records no consumer can read.
                raise PreflightError(
                    "schema_registry.mode is DEV but schema_registry.schema_id is not set; "
                    "records must carry the Confluent wire-format header",
                    catalog.SCHEMA_VALIDATION_FAILURE,
                    context={"mode": "DEV"},
                )
            logger.warning(
                "Schema Registry mode is DEV: framing records with the configured schema id %s "
                "without checking the local schema against the registry",
                schema_id,
            )
            return local_schema, schema_id, {"mode": "DEV", "schema_id": schema_id}

        client = SchemaRegistryClient(
            settings.schema_registry.urls,
            token_provider=tokens,
            ca_location=settings.schema_registry.ca_location,
            timeout=settings.schema_registry.timeout_seconds,
            backoff=BackoffPolicy(
                base_seconds=settings.resilience.backoff_base_seconds,
                max_seconds=settings.resilience.backoff_max_seconds,
            ),
            attempts=settings.schema_registry.max_attempts,
            shutdown=self._shutdown,
        )

        subject = value_subject(settings.kafka.topic)
        context: Dict[str, Any] = {}

        def resolve() -> Dict[str, Any]:
            registered = client.latest_schema(subject)
            compatible, findings = compare_schemas(local_schema, registered.schema)

            context.update(
                {
                    "subject": subject,
                    "schema_id": registered.schema_id,
                    "schema_version": registered.version,
                    "registry_url": registered.url,
                    "findings": findings,
                }
            )

            for finding in findings:
                logger.log(
                    logging.ERROR if finding.startswith("BLOCKING") else logging.INFO,
                    "Schema comparison: %s",
                    finding,
                    extra={"subject": subject},
                )

            if not compatible:
                raise PreflightError(
                    f"Local schema is incompatible with registered subject {subject}: {findings}",
                    catalog.SCHEMA_VALIDATION_FAILURE,
                    context={"subject": subject, "findings": findings},
                )

            return dict(context)

        pf.check_schema_registry(self._report, resolve=resolve)
        # A failed lookup is recorded, not raised; raise it here so a missing
        # schema id can never reach the serializer.
        self._report.raise_if_failed()

        return local_schema, context["schema_id"], context

    @staticmethod
    def _await_oauth_token(producer: Any, config: Dict[str, Any], timeout: float = 15.0) -> None:
        """Poll the new producer until the BSP ``oauth_cb`` has supplied its token.

        confluent-kafka 2.4 (the image's version) runs ``oauth_cb`` only from
        ``poll()``/``flush()``. ``list_topics()`` does not serve it, so a
        metadata request made straight after the producer is built has no token
        to authenticate with: every broker waits for one, and the request times
        out as ``_TRANSPORT``. Newer clients call it on their own, and there the
        first poll finds the token already set.
        """
        supplied = getattr(config.get("oauth_cb"), "supplied", None)
        poll = getattr(producer, "poll", None)
        if supplied is None or not callable(poll):
            return

        started = time.monotonic()
        while not supplied.is_set() and time.monotonic() - started < timeout:
            poll(0.1)

        waited_ms = round((time.monotonic() - started) * 1000)
        if supplied.is_set():
            logger.info("Kafka producer has its OAuth token for SASL after %d ms", waited_ms)
        else:
            logger.error(
                "The BSP oauth_cb supplied no usable token within %.0f s; the brokers cannot "
                "authenticate this producer",
                timeout,
            )

    def _metadata_checks(self, producer: Any) -> None:
        timeout = float(self._settings.resilience.preflight_metadata_timeout_seconds)

        def fetch(topic: str) -> Dict[str, Any]:
            try:
                metadata = producer.list_topics(topic=topic, timeout=timeout)
            except Exception as exc:
                # error_cb is only served by poll(): drain it, then name the
                # per-broker reasons, which also lets the classifier file a
                # TLS or SASL failure as authentication rather than "broker".
                poll = getattr(producer, "poll", None)
                if callable(poll):
                    poll(0)
                reasons = self._broker_errors.recent if self._broker_errors else []
                # "N/N brokers are down" only restates the failure; keep it
                # only when nothing more specific was reported.
                reasons = [r for r in reasons if "_ALL_BROKERS_DOWN" not in r] or reasons

                key_lines: List[str] = []
                if self._trace is not None:
                    # poll() above released the trace queued during the request.
                    key_lines = self._trace.key_lines(40)
                    logger.error(
                        "librdkafka trace before the failed metadata request: %d lines, %d about "
                        "TLS/SASL/auth or failures (queued until poll(), so each line's log "
                        "timestamp is when it was released, not when it happened)",
                        len(self._trace.lines),
                        len(key_lines),
                        extra={
                            "librdkafka_key_lines": key_lines,
                            "librdkafka_trace": list(self._trace.lines)[-200:],
                        },
                    )

                detail = []
                if reasons:
                    detail.append(f"broker errors: {' | '.join(reasons)}")
                if key_lines:
                    detail.append(f"last librdkafka lines: {' | '.join(key_lines[-5:])}")
                if not detail:
                    raise
                raise RuntimeError(f"{exc}; {'; '.join(detail)}") from exc
            topic_metadata = metadata.topics.get(topic)

            if topic_metadata is None:
                raise RuntimeError(f"UNKNOWN_TOPIC_OR_PART: {topic} is absent from cluster metadata")
            if topic_metadata.error is not None:
                raise RuntimeError(f"{topic_metadata.error}")
            if not topic_metadata.partitions:
                raise RuntimeError(f"UNKNOWN_TOPIC_OR_PART: {topic} has no partitions")

            return {
                "partitions": len(topic_metadata.partitions),
                "brokers": len(metadata.brokers),
            }

        pf.check_topic_metadata(
            self._report,
            fetch=fetch,
            topics=[self._settings.kafka.topic],
        )

    # -- assembly ----------------------------------------------------------

    def build(self) -> KafkaStack:
        settings = self._settings

        # First: the BSP client YAML's ssl.ca.location and the Schema Registry
        # client both read this file, and neither can be built without it.
        ca_certificate.install(settings.ca_certificate)

        bsp = self._bsp_client()
        config = self._producer_config(bsp)

        if settings.resilience.preflight_enabled:
            self._network_checks(config)
            # Stop before authenticating if the network path is already broken:
            # an auth timeout would mask the real cause.
            self._report.raise_if_failed()

        if bsp is not None:
            tokens = bsp.token_provider(
                refresh_margin_seconds=settings.schema_registry.token_refresh_margin_seconds
            )

            def acquire_token() -> Dict[str, Any]:
                tokens.get()
                return {"token_seconds_remaining": round(tokens.seconds_remaining)}

            pf.check_authentication(self._report, acquire=acquire_token)
            self._report.raise_if_failed()
        else:
            tokens = _NoTokenProvider()

        local_schema, schema_id, schema_context = self._resolve_schema(tokens)
        self._report.raise_if_failed()

        producer = self._producer_factory(config)
        # Before anything asks the brokers: SASL cannot start without it.
        self._await_oauth_token(producer, config)

        if settings.resilience.preflight_enabled and settings.resilience.preflight_metadata_enabled:
            self._metadata_checks(producer)
            self._report.raise_if_failed()
        else:
            # As produce_app does: librdkafka fetches the topic's metadata
            # itself when the first record is produced, and a missing topic or
            # ACL comes back as that record's delivery error.
            logger.info(
                "Topic metadata preflight is off; the first publish brings up the broker connection"
            )

        publisher = Publisher(
            producer,
            topic=settings.kafka.topic,
            metrics=self._metrics,
            shutdown=self._shutdown,
        )

        return KafkaStack(
            publisher=publisher,
            serializer=build_serializer(schema=local_schema, schema_id=schema_id, name="trigger"),
            size_guard=SizeGuard(settings.kafka.max_message_bytes),
            token_provider=tokens,
            schema_id=schema_id,
            preflight=self._report.to_dict(),
            producer=producer,
            schema_context=schema_context,
        )