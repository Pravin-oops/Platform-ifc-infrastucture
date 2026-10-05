"""Assembles the Kafka publishing stack and runs the readiness checks.

Everything that can fail before a record is read fails here, in the order the
POC execution plan prescribes: source readable, state store reachable, DNS, TCP,
BAM token, Schema Registry, cluster metadata. By the time ``build`` returns, the
only failures left are genuine runtime ones.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from utility.auth_helper import BSPClient, BSPTokenProvider, TokenProvider
from utility.csm_aws_fetch import CSMAuthenticator
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

        if settings.csm is not None:
            CSMAuthenticator(settings.csm).export_to_environment(settings.csm.principal_realm)
        else:
            logger.warning(
                "No CSM section configured; relying on BSP_USERNAME/BSP_PASSWORD already in the environment"
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

        if settings.schema_registry.mode == "SECURE" and settings.schema_registry.url:
            registry = [pf.parse_url_endpoint(settings.schema_registry.url)]
            pf.check_dns(self._report, registry, label="schema_registry")
            pf.check_tcp(
                self._report, registry, label="schema_registry", timeout=timeout, require_all=True
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
            settings.schema_registry.url or "",
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

    def _metadata_checks(self, producer: Any) -> None:
        timeout = float(self._settings.resilience.preflight_timeout_seconds)

        def fetch(topic: str) -> Dict[str, Any]:
            metadata = producer.list_topics(topic=topic, timeout=timeout)
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

        if settings.resilience.preflight_enabled:
            self._metadata_checks(producer)
            self._report.raise_if_failed()

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