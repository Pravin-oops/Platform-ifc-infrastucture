"""Typed configuration for the IFC trigger connector."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from utility.trigger_definitions import resolve as resolve_trigger

# Broker-enforced ceiling documented in the TBB failure catalogue (Message Too
# Large). Kept just under 800 KiB so the Avro envelope and headers still fit.
DEFAULT_MAX_MESSAGE_BYTES = 800 * 1024


class AppSettings(BaseModel):
    name: str = "ifc-trigger-connector"
    #: Selects the envelope's triggerOriginatingSystem and idSystem.
    environment: str = "UAT"
    log_level: str = "INFO"


#: triggerOriginatingSystem and idSystem for each app.environment: the
#: environment's service number.
DEFAULT_ORIGINATING_SYSTEMS: Dict[str, str] = {
    "DEV": "SNSVC0084379",
    "SIT": "SNSVC0084378",
    "PROD-ANALYTICS": "SNSVC0084375",
    "PROD-PARALLEL": "SNSVC0084371",
    "PROD": "SNSVC0084373",
}

_SYSTEM_CODE = re.compile(r"^[A-Za-z0-9-]+$")


class EnvelopeSettings(BaseModel):
    #: app.environment -> service number sent as triggerOriginatingSystem and idSystem.
    originating_systems: Dict[str, str] = Field(default_factory=lambda: dict(DEFAULT_ORIGINATING_SYSTEMS))
    #: Environments whose data is tokenised; only there do payload fields declare a policy.
    tokenised_environments: List[str] = Field(default_factory=lambda: ["PROD"])

    @field_validator("tokenised_environments", mode="before")
    @classmethod
    def _split_a_string(cls, value: Any) -> Any:
        """Accept ``DEV,PROD`` as well as a list, so the ECS task can set
        IFC_ENVELOPE__TOKENISED_ENVIRONMENTS without writing JSON."""
        if isinstance(value, str):
            return value.split(",")
        return value

    @field_validator("tokenised_environments")
    @classmethod
    def _upper(cls, value: List[str]) -> List[str]:
        return sorted({str(env).strip().upper() for env in value if str(env).strip()})

    @field_validator("originating_systems")
    @classmethod
    def _normalise(cls, value: Dict[str, str]) -> Dict[str, str]:
        systems = {str(env).strip().upper(): str(code).strip() for env, code in value.items()}
        # The code is the trigger ID's first part, which is '_'-separated.
        bad = {env: code for env, code in systems.items() if not _SYSTEM_CODE.match(code)}
        if bad:
            raise ValueError(f"envelope.originating_systems codes must be letters, digits or '-': {bad}")
        return systems


class RunSettings(BaseModel):
    """One invocation publishes one trigger's month and exits (ECS RunTask
    under EventBridge Scheduler)."""

    shutdown_grace_seconds: int = Field(
        default=90,
        ge=5,
        description="Must be <= the ECS task definition stopTimeout, or SIGKILL wins.",
    )
    #: Which trigger this invocation publishes. EventBridge Scheduler supplies it
    #: on every invocation as ``IFC_RUN__TRIGGER``; a run without it fails.
    trigger: Optional[str] = None
    #: Bypass the weekend and already-delivered gates. Operator decision for a
    #: re-delivery, never a scheduled value - see ``run_gate``.
    force: bool = False
    #: Reprocess a past month (YYYY-MM or MONTH_YYYY) instead of the current one.
    #: A month already delivered also needs ``force``.
    month: Optional[str] = None

    @field_validator("trigger", mode="before")
    @classmethod
    def _canonical_trigger(cls, v: Any) -> Optional[str]:
        return canonical_trigger(v)

    @field_validator("month", mode="before")
    @classmethod
    def _month(cls, v: Any) -> Optional[str]:
        if v is None or (isinstance(v, str) and not v.strip()):
            return None
        from utility.run_gate import parse_month

        return parse_month(v).strftime("%Y-%m")


class RunMarkerSettings(BaseModel):
    """Where the record of each invocation's outcome is appended."""

    path: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _no_table(cls, data: Any) -> Any:
        # Naming the removed DynamoDB table fails, rather than silently disabling the gate.
        if isinstance(data, dict) and data.get("table_name"):
            raise ValueError(
                "run_marker.table_name is no longer supported (DynamoDB is not used); "
                "set run_marker.path to an s3:// URI or local file for the run marker JSON file"
            )
        return data

    @field_validator("path", mode="before")
    @classmethod
    def _strip(cls, v: Any) -> Any:
        if isinstance(v, str):
            v = v.strip()
            return v or None
        return v


def canonical_trigger(value: Any) -> Optional[str]:
    """``'trigger 9'``, ``'TRIGGER-9'``, ``9`` -> ``'TRIGGER_9'``."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return resolve_trigger(value).sub_type
    except KeyError as exc:
        raise ValueError(str(exc)) from None


#: A bare SQL identifier. Table and column names cannot be bound as query
#: parameters, so they are checked against this before being quoted into SQL.
_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def sql_identifier(value: str, *, what: str) -> str:
    """``value`` double-quoted for Athena, or ``ValueError`` if it is not a plain name."""
    if not isinstance(value, str) or not _SQL_IDENTIFIER.match(value):
        raise ValueError(f"{what} {value!r} is not a plain SQL identifier")
    return f'"{value}"'


def athena_table(name: str) -> str:
    """``database.table`` as a quoted, validated SQL table reference."""
    parts = name.split(".") if isinstance(name, str) else []
    if len(parts) != 2:
        raise ValueError(f"Athena source {name!r} must be named as database.table")
    database, table = parts
    return (
        f"{sql_identifier(database, what='Athena database')}."
        f"{sql_identifier(table, what='Athena table')}"
    )


class AthenaSettings(BaseModel):
    """How the trigger tables are queried through Athena."""

    workgroup: str = "primary"
    catalog: str = "AwsDataCatalog"
    #: Where Athena writes result files. ``None`` defers to the workgroup, which
    #: is the right place to enforce the location and its KMS key.
    output_location: Optional[str] = None
    #: Upstream stamps each month's rows with its last day; a run reads the previous month's.
    business_date_column: str = "business_date"
    #: Fixed row order, so a re-run allocates the same sequence numbers.
    order_by: List[str] = Field(default_factory=list)
    poll_interval_seconds: float = Field(default=1.0, gt=0)
    query_timeout_seconds: int = Field(default=300, ge=10)

    @model_validator(mode="after")
    def _identifiers(self) -> "AthenaSettings":
        sql_identifier(self.business_date_column, what="source.athena.business_date_column")
        for column in self.order_by:
            sql_identifier(column, what="source.athena.order_by column")
        return self


class SourceSettings(BaseModel):
    """Where trigger events come from: one Athena (Iceberg) table per trigger."""

    model_config = ConfigDict(extra="forbid")

    athena: AthenaSettings = Field(default_factory=AthenaSettings)
    #: Per-trigger table as ``database.table``, keyed by trigger (any accepted
    #: spelling). ``IFC_RUN__TRIGGER`` decides which one a run queries.
    trigger_tables: Dict[str, str] = Field(default_factory=dict)
    #: Fallback when the trigger has no ``trigger_tables`` entry; unset in production.
    table: Optional[str] = None

    @field_validator("trigger_tables", mode="before")
    @classmethod
    def _canonical_keys(cls, v: Any) -> Any:
        if not isinstance(v, dict):
            return v
        return {canonical_trigger(key): table for key, table in v.items()}

    @model_validator(mode="after")
    def _require_a_table(self) -> "SourceSettings":
        if not self.table and not self.trigger_tables:
            raise ValueError("source.trigger_tables (or source.table) is required")
        for name in [self.table, *self.trigger_tables.values()]:
            if name is not None:
                athena_table(name)
        return self

    @property
    def resolved_table(self) -> str:
        """The ``database.table`` this run queries."""
        if not self.table:
            raise ValueError(
                "source.table is not resolved: set IFC_RUN__TRIGGER so the trigger's "
                "source.trigger_tables entry is used"
            )
        return self.table


class ReconSettings(BaseModel):
    """Where upstream's reconciliation rows are read from: one Athena table."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    #: The recon table, as ``database.table``.
    table: Optional[str] = None
    #: Per trigger, the ``target_table_name`` its recon rows carry.
    trigger_targets: Dict[str, str] = Field(default_factory=dict)
    #: This run's ``target_table_name``, set from ``trigger_targets`` by
    #: ``ConnectorSettings.select_trigger``.
    target_table: Optional[str] = None

    @field_validator("trigger_targets", mode="before")
    @classmethod
    def _canonical_keys(cls, v: Any) -> Any:
        if not isinstance(v, dict):
            return v
        return {canonical_trigger(key): target for key, target in v.items()}

    @field_validator("table")
    @classmethod
    def _table(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            athena_table(v)
        return v


class KafkaSettings(BaseModel):
    topic: str
    #: BSP client YAML, local path or s3://. Omit only for a local broker set in ``overrides``.
    bsp_config_path: Optional[str] = None
    #: app.environment -> BSP client YAML; IFC_KAFKA__BSP_CONFIG_PATH overrides it.
    bsp_config_paths: Dict[str, str] = Field(default_factory=dict)
    max_message_bytes: int = Field(default=DEFAULT_MAX_MESSAGE_BYTES, ge=1024)
    flush_timeout_seconds: int = Field(default=120, ge=1)
    local_queue_max_messages: int = Field(default=20000, ge=100)
    #: Extra librdkafka properties merged over whatever the BSP client returns.
    #: Anything security-related is deliberately left to BSP.
    overrides: Dict[str, Any] = Field(default_factory=dict)
    #: librdkafka debug contexts (e.g. ``security,broker``), for diagnosis only.
    #: ``all`` and ``conf`` are refused: they print the config.
    debug: Optional[str] = None

    @field_validator("debug")
    @classmethod
    def _no_config_dump(cls, value: Optional[str]) -> Optional[str]:
        contexts = {part.strip().lower() for part in (value or "").split(",") if part.strip()}
        if contexts & {"all", "conf"}:
            raise ValueError("kafka.debug must not include 'all' or 'conf': they print the client config")
        return ",".join(sorted(contexts)) or None


def _join_url_list(value: Any) -> Any:
    """A YAML list of registry URLs, as the comma-separated string the rest expects."""
    if isinstance(value, (list, tuple)):
        return ",".join(str(item) for item in value)
    return value


class SchemaRegistryModeSettings(BaseModel):
    """Values for one registry mode. Each one set here overrides the shared
    ``schema_registry`` value while that mode is active; unset ones fall back."""

    url: Optional[str] = None
    ca_location: Optional[str] = None
    timeout_seconds: Optional[int] = Field(default=None, ge=1)
    token_refresh_margin_seconds: Optional[int] = Field(default=None, ge=30)
    max_attempts: Optional[int] = Field(default=None, ge=1)
    schema_path: Optional[str] = None
    schema_id: Optional[int] = Field(default=None, ge=1)

    @field_validator("url", mode="before")
    @classmethod
    def _join_a_list(cls, value: Any) -> Any:
        return _join_url_list(value)


class SchemaRegistrySettings(BaseModel):
    #: SECURE: 8095 registry with the BAM token. DEV: 8082 registry without one, or a pinned id.
    mode: Literal["DEV", "SECURE"] = "SECURE"
    #: Per-mode values; the block for ``mode`` overrides the shared values
    #: below, so the rest of the connector reads one set of settings.
    dev: Optional[SchemaRegistryModeSettings] = None
    secure: Optional[SchemaRegistryModeSettings] = None
    #: One or more comma-separated registry URLs; the first that answers is used.
    url: Optional[str] = None
    ca_location: Optional[str] = None
    timeout_seconds: int = Field(default=30, ge=1)
    #: Refresh the SR bearer token this many seconds before its ``exp`` claim.
    token_refresh_margin_seconds: int = Field(default=300, ge=30)
    #: Attempts at each registry request (schema lookup) before preflight fails,
    #: backing off per ``resilience.backoff_*_seconds``.
    max_attempts: int = Field(default=5, ge=1)
    #: Local .avsc used to serialise. Compared against the registered subject at
    #: preflight so producer/registry drift fails before any publish happens.
    schema_path: str = "utility/schema.json"
    #: DEV only: the schema id to frame records with instead of looking it up.
    schema_id: Optional[int] = Field(default=None, ge=1)

    @field_validator("mode", mode="before")
    @classmethod
    def _mode_any_case(cls, value: Any) -> Any:
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("url", mode="before")
    @classmethod
    def _join_a_list(cls, value: Any) -> Any:
        return _join_url_list(value)

    @model_validator(mode="after")
    def _apply_mode_block(self) -> "SchemaRegistrySettings":
        block = self.dev if self.mode == "DEV" else self.secure
        if block is not None:
            for name, value in block.model_dump(exclude_none=True).items():
                setattr(self, name, value)
        return self

    @property
    def urls(self) -> List[str]:
        """The registry URLs in configured order, without trailing slashes."""
        return [part.strip().rstrip("/") for part in (self.url or "").split(",") if part.strip()]

    @model_validator(mode="after")
    def _require_url_when_secure(self) -> "SchemaRegistrySettings":
        if self.mode == "SECURE" and not self.urls:
            raise ValueError("schema_registry.url (or schema_registry.secure.url) is required when mode=SECURE")
        malformed = [u for u in self.urls if not re.match(r"^https?://[^/\s:]+", u)]
        if malformed:
            raise ValueError(f"schema_registry.url entries must be http(s) URLs: {malformed}")
        return self


class CaCertificateSettings(BaseModel):
    """The Barclays root CA, fetched from Secrets Manager at container start."""

    #: Secrets Manager name or ARN of the CA certificate, a plain PEM.
    secret_id: Optional[str] = None
    secret_region: str = "eu-west-1"
    #: Where the CA is written. /tmp, because the container runs as a
    #: non-root user that cannot write anywhere else outside its home.
    path: str = "/tmp/ifc-certs/CARoot.pem"


#: Environment variables the ECS product template sets from its CyberArk
#: parameters, and the ``cyberark`` field each one fills.
CYBERARK_ENV: Dict[str, str] = {
    "CYBERARK_ENABLED": "enabled",
    "CYBERARK_CCP_URL": "base_url",
    "CYBERARK_APP_ID": "app_id",
    "CYBERARK_SAFE": "safe",
    "CYBERARK_ACCOUNT": "object",
}


class CyberArkSettings(BaseModel):
    """Where the BSP system-account credential lives in CyberArk."""

    #: Off: CCP is not called and BSP_USERNAME/BSP_PASSWORD come from the environment.
    enabled: bool = True
    #: CCP host, or the full ``.../AIMWebService_certs/api/Accounts`` URL; the
    #: fetcher appends the endpoint path only when it is not already there.
    base_url: Optional[str] = None
    #: Application ID registered with CyberArk (``APP_<name>``).
    app_id: Optional[str] = None
    #: Safe that holds the account.
    safe: Optional[str] = None
    folder: str = "Root"
    #: The account's name in the Safe, sent as CCP's ``Object``.
    object: Optional[str] = None
    #: Secrets Manager name or ARN of the client certificate, a plain PEM.
    client_cert_secret_id: str
    #: Secrets Manager name or ARN of the client private key, a plain
    #: unencrypted PEM.
    client_key_secret_id: str
    secret_region: str = "eu-west-1"
    #: Trust store for the CCP *server* certificate.
    ca_bundle_path: str = "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"
    ssl_verify: bool = True
    request_timeout: int = 30
    #: Attempts at the credential when CCP or Secrets Manager is unavailable.
    #: A rejected certificate or query fails on the first attempt.
    max_attempts: int = Field(default=3, ge=1)
    #: Realm appended to the CyberArk username to form the BSP principal.
    principal_realm: str = "@INTRANET.BARCAPINT.COM"

    @model_validator(mode="after")
    def _query_is_complete(self) -> "CyberArkSettings":
        if self.enabled:
            missing = [
                f"cyberark.{name} ({env})"
                for env, name in CYBERARK_ENV.items()
                if name != "enabled" and not getattr(self, name)
            ]
            if missing:
                raise ValueError(f"CyberArk is enabled but these are not set: {', '.join(missing)}")
        return self


class AuditSettings(BaseModel):
    #: Bucket name or s3://bucket/folder; the prefixes below are appended to it.
    bucket: Optional[str] = None
    manifest_prefix: str = "triggerbackbone/ifc/manifests/"
    quarantine_prefix: str = "triggerbackbone/ifc/quarantine/"
    payload_prefix: str = "triggerbackbone/ifc/payloads/"
    #: Writing every serialised record to S3 is control evidence but costs one
    #: PUT per message; disable for high-volume environments.
    write_payloads: bool = False

    @field_validator("bucket", mode="before")
    @classmethod
    def _strip(cls, v: Any) -> Any:
        return v.strip().rstrip("/") if isinstance(v, str) else v

    @property
    def root(self) -> str:
        """``bucket`` as an ``s3://`` URI, however it was written."""
        if not self.bucket:
            raise ValueError("audit.bucket is not configured")
        if self.bucket.startswith("s3://"):
            return self.bucket
        return f"s3://{self.bucket}"


class ResilienceSettings(BaseModel):
    #: Attempts at publishing each record, backing off per ``backoff_*_seconds``;
    #: every failed attempt counts towards the circuit breaker.
    max_publish_attempts: int = Field(default=5, ge=1)
    backoff_base_seconds: float = Field(default=1.0, gt=0)
    backoff_max_seconds: float = Field(default=60.0, gt=0)
    circuit_breaker_threshold: int = Field(
        default=20, ge=1, description="Consecutive infra failures before the run is abandoned."
    )
    circuit_breaker_reset_seconds: float = Field(default=120.0, gt=0)
    preflight_enabled: bool = True
    #: Per-endpoint DNS/TCP connect timeout at preflight: how long an
    #: unreachable endpoint takes to report.
    preflight_timeout_seconds: int = Field(default=30, ge=1)
    #: On: list_topics() before any record is read, to fail fast. Off: the first publish connects.
    preflight_metadata_enabled: bool = False
    #: How long the metadata request may take. librdkafka retries the TLS and
    #: SASL handshakes across the brokers within it, so it is the longer one.
    preflight_metadata_timeout_seconds: int = Field(default=60, ge=1)
    #: Fail the run if more than this fraction of records is quarantined, so a
    #: run cannot silently publish a fraction of its content and report success.
    max_quarantine_ratio: float = Field(default=0.05, ge=0.0, le=1.0)


class HealthSettings(BaseModel):
    enabled: bool = True
    port: int = Field(default=8080, ge=1, le=65535)
    bind_host: str = "0.0.0.0"


class NotificationSettings(BaseModel):
    sns_topic_arn: Optional[str] = None
    #: Included in the alert subject so RTB can route without opening the body.
    application_label: str = "IFC Trigger Connector"
    #: Trigger Backbone batch-completion topic (separate from the RTB failure topic).
    batch_sns_topic_arn: Optional[str] = None
    #: Published on the completion event as ``Trigger_Originating_BU``.
    trigger_originating_bu: str = "UK-C"
    batch_notifications_enabled: bool = True


class ConnectorSettings(BaseModel):
    app: AppSettings = Field(default_factory=AppSettings)
    run: RunSettings = Field(default_factory=RunSettings)
    source: SourceSettings
    kafka: KafkaSettings
    schema_registry: SchemaRegistrySettings
    ca_certificate: CaCertificateSettings = Field(default_factory=CaCertificateSettings)
    cyberark: Optional[CyberArkSettings] = None
    run_marker: RunMarkerSettings = Field(default_factory=RunMarkerSettings)
    audit: AuditSettings = Field(default_factory=AuditSettings)
    resilience: ResilienceSettings = Field(default_factory=ResilienceSettings)
    health: HealthSettings = Field(default_factory=HealthSettings)
    notifications: NotificationSettings = Field(default_factory=NotificationSettings)
    recon: ReconSettings = Field(default_factory=ReconSettings)
    envelope: EnvelopeSettings = Field(default_factory=EnvelopeSettings)

    @field_validator("app")
    @classmethod
    def _upper_log_level(cls, v: AppSettings) -> AppSettings:
        v.log_level = v.log_level.upper()
        return v

    @model_validator(mode="after")
    def _cross_checks(self) -> "ConnectorSettings":
        if self.audit.write_payloads and not self.audit.bucket:
            raise ValueError("audit.bucket is required when audit.write_payloads is true")
        if not self.kafka.bsp_config_path and not self.kafka.overrides.get("bootstrap.servers"):
            raise ValueError(
                "kafka.bsp_config_path is required, or kafka.overrides must supply bootstrap.servers "
                "for a direct (non-BSP) broker connection"
            )
        if not self.kafka.bsp_config_path and self.schema_registry.mode == "SECURE":
            raise ValueError(
                "schema_registry.mode=SECURE needs a BAM token, which only the BSP client provides; "
                "set kafka.bsp_config_path"
            )
        return self

    @property
    def recon_active(self) -> bool:
        """Whether the upstream reconciliation gate applies to this run."""
        if not self.recon.enabled:
            return False
        target = self.recon.target_table or self.recon.trigger_targets.get(self.run.trigger or "")
        return bool(self.recon.table and target)

    @property
    def originating_system(self) -> str:
        """The environment's service number: triggerOriginatingSystem and idSystem."""
        environment = self.app.environment.strip().upper()
        try:
            return self.envelope.originating_systems[environment]
        except KeyError:
            raise ValueError(
                f"app.environment {self.app.environment!r} has no envelope.originating_systems "
                f"entry, so triggerOriginatingSystem/idSystem cannot be set; known: "
                f"{sorted(self.envelope.originating_systems)}"
            ) from None

    @property
    def declares_encryption_policies(self) -> bool:
        """Whether payload fields carry their tokenisation policy in this environment."""
        return self.app.environment.strip().upper() in self.envelope.tokenised_environments

    @property
    def gate_active(self) -> bool:
        """Whether the entry-point run gate applies: only with a marker file."""
        return bool(self.run_marker.path)

    @property
    def trigger(self) -> str:
        """This run's trigger, as a ``str``."""
        if not self.run.trigger:
            raise ValueError(
                "No trigger specified: set IFC_RUN__TRIGGER (TRIGGER_8 | TRIGGER_9 | TRIGGER_21)"
            )
        return self.run.trigger

    def select_trigger(self, trigger: Any) -> str:
        """Fix this run's trigger and point the source at that trigger's table."""
        if trigger is not None:
            self.run.trigger = canonical_trigger(trigger)
        selected = self.trigger

        if selected in self.source.trigger_tables:
            self.source.table = self.source.trigger_tables[selected]

        # The recon rows are looked up by the same trigger, so the table read and
        # the reconciliation checked cannot end up being different triggers'.
        if selected in self.recon.trigger_targets:
            self.recon.target_table = self.recon.trigger_targets[selected]

        if not self.source.table:
            raise ValueError(
                f"No Athena table for trigger {selected!r}: add it to source.trigger_tables"
            )
        return selected



_ENV_PREFIX = "IFC_"
_SECTION_SEP = "__"


def _coerce(raw: str) -> Any:
    """Environment values arrive as strings; recover ints/bools/JSON where obvious."""
    lowered = raw.strip().lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none", ""}:
        return None
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return raw


#: Settings where an empty IFC_ variable means unset; elsewhere empty means null.
_EMPTY_MEANS_UNSET = {
    ("schema_registry", "mode"),
    ("app", "environment"),
    ("envelope", "tokenised_environments"),
}


def _env_overlay() -> Dict[str, Any]:
    overlay: Dict[str, Any] = {}
    for key, value in os.environ.items():
        if not key.startswith(_ENV_PREFIX):
            continue
        path = key[len(_ENV_PREFIX) :].lower().split(_SECTION_SEP)
        if not value.strip() and tuple(path) in _EMPTY_MEANS_UNSET:
            continue
        cursor: Any = overlay
        for part in path[:-1]:
            nxt = cursor.setdefault(part, {})
            if not isinstance(nxt, dict):
                # A scalar already claimed this path; the more specific key wins.
                nxt = {}
                cursor[part] = nxt
            cursor = nxt
        cursor[path[-1]] = _coerce(value)
    return overlay


def _cyberark_env_overlay() -> Dict[str, Any]:
    """The template's ``CYBERARK_*`` variables, as a ``cyberark`` section."""
    section = {
        field: os.environ[env].strip()
        for env, field in CYBERARK_ENV.items()
        if os.environ.get(env, "").strip()
    }
    return {"cyberark": section} if section else {}


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _select_bsp_config(layered: Dict[str, Any]) -> None:
    """Put this environment's BSP client YAML into kafka.bsp_config_path."""
    if os.environ.get(f"{_ENV_PREFIX}KAFKA{_SECTION_SEP}BSP_CONFIG_PATH", "").strip():
        return
    kafka = layered.get("kafka")
    if not isinstance(kafka, dict) or not isinstance(kafka.get("bsp_config_paths"), dict):
        return
    environment = str((layered.get("app") or {}).get("environment") or AppSettings().environment)
    by_environment = {str(env).strip().upper(): path for env, path in kafka["bsp_config_paths"].items()}
    chosen = by_environment.get(environment.strip().upper())
    if chosen:
        kafka["bsp_config_path"] = chosen


def load_settings(config_path: str, *, reader=None) -> ConnectorSettings:
    """Read the YAML config, apply the ``IFC_`` env overlay, validate."""
    if reader is None:
        from utility.connector_utility import read_text

        reader = read_text

    document = yaml.safe_load(reader(config_path)) or {}
    if not isinstance(document, dict):
        raise ValueError(f"Config at {config_path} is not a YAML mapping")

    # IFC_ variables are applied last, so IFC_CYBERARK__* still overrides the
    # template's CYBERARK_* for a single task.
    layered = _deep_merge(_deep_merge(document, _cyberark_env_overlay()), _env_overlay())
    _select_bsp_config(layered)
    settings = ConnectorSettings.model_validate(layered)

    # The month is read deep inside path expansion and the envelope builder,
    # which are not handed the settings, so it is pinned for the process here.
    from utility.run_gate import set_execution_month

    set_execution_month(settings.run.month)
    return settings