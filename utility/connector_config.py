"""Typed configuration for the IFC trigger connector.

Config is layered, lowest precedence first:

  1. defaults declared on the models below
  2. the YAML document named by ``--config`` / ``APP_CONFIG_PATH`` (local path or s3://)
  3. ``IFC_`` environment variables (double underscore separates sections,
     e.g. ``IFC_KAFKA__TOPIC``) so an ECS task definition can override any
     single value without republishing the config object to S3.

Secrets never live here. CSM supplies the BSP system-account credentials at
runtime; only the *location* of the secret is configuration.
"""

from __future__ import annotations

import json
import os
from datetime import date
from typing import Any, Dict, List, Literal, Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from ifc_trigger_connector.utility.trigger_definitions import resolve as resolve_trigger

# Broker-enforced ceiling documented in the TBB failure catalogue (Message Too
# Large). Kept just under 800 KiB so the Avro envelope and headers still fit.
DEFAULT_MAX_MESSAGE_BYTES = 800 * 1024


#: Date tokens accepted in ``source.path`` / ``source.trigger_paths``, so one
#: config points at a new folder every month without being republished. TED
#: writes ``trigger_8/SEPTEMBER_2026/``, which is ``{MONTH}_{YYYY}``.
#:
#: The date substituted is the *run* date, so a run in September 2026 reads
#: ``SEPTEMBER_2026``. That is the execution month the run gate keys on, not the
#: business month the records describe - a September run publishes August
#: business data out of the September folder.
_DATE_TOKENS = {
    "{MONTH}": lambda d: d.strftime("%B").upper(),
    "{Month}": lambda d: d.strftime("%B"),
    "{month}": lambda d: d.strftime("%B").lower(),
    "{MON}": lambda d: d.strftime("%b").upper(),
    "{YYYY}": lambda d: d.strftime("%Y"),
    "{YY}": lambda d: d.strftime("%y"),
    "{MM}": lambda d: d.strftime("%m"),
    "{DD}": lambda d: d.strftime("%d"),
    "{YYYYMM}": lambda d: d.strftime("%Y%m"),
    "{YYYY-MM}": lambda d: d.strftime("%Y-%m"),
    "{YYYYMMDD}": lambda d: d.strftime("%Y%m%d"),
}


def expand_date_tokens(template: str, run_date: Optional[date] = None) -> str:
    """Substitute the date tokens in a source path.

    ``%B`` is locale-sensitive in principle; the container runs under the C
    locale, where it is English, which is what TED writes.
    """
    if not template or "{" not in template:
        return template

    if run_date is None:
        from ifc_trigger_connector.utility.run_gate import today

        run_date = today()

    resolved = template
    for token, render in _DATE_TOKENS.items():
        if token in resolved:
            resolved = resolved.replace(token, render(run_date))
    return resolved


class AppSettings(BaseModel):
    name: str = "ifc-trigger-connector"
    environment: str = "UAT"
    log_level: str = "INFO"
    # Envelope identity values (triggerType, triggerOriginatingSystem, idSystem,
    # ...) are constants in tb_outcome_schema, not configuration.


class RunSettings(BaseModel):
    #: ``batch``   - drain the source once and exit. Use with ECS RunTask /
    #:               EventBridge Scheduler for the monthly cadence.
    #: ``service`` - stay resident and re-poll the source. Use with an ECS
    #:               service when triggers arrive continuously.
    mode: Literal["batch", "service"] = "batch"
    poll_interval_seconds: int = Field(default=300, ge=5)
    max_batches: int = Field(default=0, ge=0, description="0 = unbounded (service mode only).")
    shutdown_grace_seconds: int = Field(
        default=90,
        ge=5,
        description="Must be <= the ECS task definition stopTimeout, or SIGKILL wins.",
    )
    #: Which trigger this invocation publishes. Normally supplied per-invocation
    #: by the scheduler's RunTask override (``IFC_RUN__TRIGGER``) rather than set here; the
    #: config value is the local-run convenience.
    trigger: Optional[str] = None
    #: Bypass the weekend and already-delivered gates. Operator decision for a
    #: re-delivery, never a scheduled value - see ``run_gate``.
    force: bool = False

    @field_validator("trigger", mode="before")
    @classmethod
    def _canonical_trigger(cls, v: Any) -> Optional[str]:
        return canonical_trigger(v)


class RunMarkerSettings(BaseModel):
    """Where the record of each invocation's outcome is appended.

    A JSON Lines file - an ``s3://bucket/folder/run_markers.json`` URI, or a
    local path - that the run gate reads to decide whether this month is
    already delivered, and that Athena queries as a table over the folder.
    Leave ``path`` unset to switch the gate off (local runs).
    """

    path: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _no_table(cls, data: Any) -> Any:
        # The DynamoDB table is gone. A config still naming it must fail rather
        # than be ignored: an ignored key leaves ``path`` unset, which switches
        # the gate off and lets every date in the window republish the month.
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
    """``'trigger 9'``, ``'TRIGGER-9'``, ``9`` -> ``'TRIGGER_9'``.

    The scheduler's spelling is not ours to control, but the run marker key and
    the source lookup must agree on one, or 'trigger 9' and 'TRIGGER_9' would
    each deliver the same month.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return resolve_trigger(value).sub_type
    except KeyError as exc:
        raise ValueError(str(exc)) from None


class SourceSettings(BaseModel):
    type: Literal["s3", "local"] = "s3"
    #: Fallback location, used when the run's trigger has no ``trigger_paths`` entry.
    path: Optional[str] = None
    #: Per-trigger source location, keyed by trigger (any accepted spelling).
    #: The scheduler names the trigger; this decides where its data is read from.
    trigger_paths: Dict[str, str] = Field(default_factory=dict)
    archive_path: Optional[str] = Field(
        default=None,
        description="Where a successfully drained object is copied. None = leave in place.",
    )
    #: ``None`` (the default) publishes every record the source holds. A number
    #: caps the batch, which only makes sense when the remainder can be picked
    #: up later - that is, when the source is a prefix of several objects. The
    #: ECS contract is one file holding the whole batch, where a cap would
    #: silently drop its tail, so production leaves this unset and a cap is a
    #: developer convenience for bounding a local run.
    max_records_per_batch: Optional[int] = Field(default=None, ge=1)
    file_suffixes: List[str] = Field(default_factory=lambda: [".json", ".jsonl"])
    #: ``latest`` reads only the newest object under the resolved folder;
    #: ``all`` reads every one. TED writes the whole month as one timestamped
    #: extract and rewrites it rather than appending, so an older file in the
    #: folder is a superseded draft - reading them all would republish it.
    selection: Literal["latest", "all"] = "latest"
    #: How the timestamp is found in a filename, and how to read it. The default
    #: matches ``trigger8_20260930_143022_123456.json``. Filenames are ordered by
    #: the *parsed* timestamp, never by name: a day-first or month-first format
    #: does not sort lexicographically, and a name sort would quietly select a
    #: month-old extract.
    filename_timestamp_pattern: str = r"(\d{8}_\d{6}_\d+)"
    filename_timestamp_format: str = "%Y%m%d_%H%M%S_%f"

    @field_validator("trigger_paths", mode="before")
    @classmethod
    def _canonical_keys(cls, v: Any) -> Any:
        if not isinstance(v, dict):
            return v
        return {canonical_trigger(key): path for key, path in v.items()}

    @model_validator(mode="after")
    def _require_a_location(self) -> "SourceSettings":
        if not self.path and not self.trigger_paths:
            raise ValueError("source.path or source.trigger_paths is required")
        return self

    @property
    def resolved_path(self) -> str:
        """The location this run reads, with its date tokens expanded.

        ``path`` is optional in config because a trigger's ``trigger_paths``
        entry can supply it, but by the time the source is read
        ``ConnectorSettings.select_trigger`` must have resolved it.

        Expansion happens here rather than once at load, so a resident service
        that crosses midnight - or a month boundary - moves to the new folder
        without a restart.
        """
        if not self.path:
            raise ValueError(
                "source.path is not resolved: set IFC_RUN__TRIGGER so the trigger's "
                "source.trigger_paths entry is used, or set source.path"
            )
        return expand_date_tokens(self.path)


class ReconSettings(BaseModel):
    """Where the upstream reconciliation document for a trigger lives.

    Same shape as ``SourceSettings``: a per-trigger location carrying the same
    ``{MONTH}_{YYYY}`` date tokens, and the same newest-file-wins rule keyed on
    the timestamp in the filename. ``enabled: false`` switches the gate off for
    a local run that has no recon feed.
    """

    enabled: bool = True
    path: Optional[str] = None
    trigger_paths: Dict[str, str] = Field(default_factory=dict)
    file_suffixes: List[str] = Field(default_factory=lambda: [".json"])
    #: Matches BDP_Corp_Trigger_8_recon_20260930_143022_123456.json.
    filename_timestamp_pattern: str = r"(\d{8}_\d{6}_\d+)"
    filename_timestamp_format: str = "%Y%m%d_%H%M%S_%f"

    @field_validator("trigger_paths", mode="before")
    @classmethod
    def _canonical_keys(cls, v: Any) -> Any:
        if not isinstance(v, dict):
            return v
        return {canonical_trigger(key): path for key, path in v.items()}

    @property
    def resolved_path(self) -> str:
        """The recon location for this run, tokens still unexpanded.

        ``ReconSource`` expands them, so a resident service crossing a month
        boundary looks in the new month's folder without a restart.
        """
        if not self.path:
            raise ValueError(
                "recon.path is not resolved: add the trigger to recon.trigger_paths, "
                "set recon.path, or set recon.enabled=false"
            )
        return self.path


class KafkaSettings(BaseModel):
    topic: str
    #: BSP client YAML (librdkafka props); local path or s3://. Omit only for a
    #: local broker, where ``overrides`` supplies bootstrap.servers directly and
    #: the BSP client is not involved at all.
    bsp_config_path: Optional[str] = None
    max_message_bytes: int = Field(default=DEFAULT_MAX_MESSAGE_BYTES, ge=1024)
    flush_timeout_seconds: int = Field(default=120, ge=1)
    local_queue_max_messages: int = Field(default=20000, ge=100)
    #: Extra librdkafka properties merged over whatever the BSP client returns.
    #: Anything security-related is deliberately left to BSP.
    overrides: Dict[str, Any] = Field(default_factory=dict)


class SchemaRegistrySettings(BaseModel):
    mode: Literal["DEV", "SECURE"] = "SECURE"
    url: Optional[str] = None
    ca_location: Optional[str] = None
    timeout_seconds: int = Field(default=30, ge=1)
    #: Refresh the SR bearer token this many seconds before its ``exp`` claim.
    token_refresh_margin_seconds: int = Field(default=300, ge=30)
    #: Local .avsc used to serialise. Compared against the registered subject at
    #: preflight so producer/registry drift fails before any publish happens.
    schema_path: str = "utility/schema.json"

    @model_validator(mode="after")
    def _require_url_when_secure(self) -> "SchemaRegistrySettings":
        if self.mode == "SECURE" and not self.url:
            raise ValueError("schema_registry.url is required when mode=SECURE")
        return self


class CSMSettings(BaseModel):
    base_url: str
    mount_point: str = "CSM"
    secret_path: str
    role_name: str
    ca_bundle_path: str = "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"
    ssl_verify: bool = True
    sts_region: str = "us-east-1"
    instance_region: str = "eu-west-1"
    vault_server_id: str = "CSM_PROD"
    request_timeout: int = 30
    #: Realm appended to the CSM username to form the BSP principal.
    principal_realm: str = "@INTRANET.BARCAPINT.COM"


class AuditSettings(BaseModel):
    #: Where the audit evidence is written. Either a bare bucket name, or a full
    #: ``s3://bucket/folder`` URI when the evidence lives under a folder rather
    #: than at the root of a bucket of its own. The three prefixes below are
    #: appended to it, so a URI with a folder nests them under that folder.
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
        """``bucket`` as an ``s3://`` URI, however it was written.

        A bare bucket name gains the scheme; a URI keeps whatever folder it
        already names. Normalising here rather than at each call site is what
        stops a configured ``s3://...`` value becoming ``s3://s3://...``.
        """
        if not self.bucket:
            raise ValueError("audit.bucket is not configured")
        if self.bucket.startswith("s3://"):
            return self.bucket
        return f"s3://{self.bucket}"


class ResilienceSettings(BaseModel):
    max_publish_attempts: int = Field(default=5, ge=1)
    backoff_base_seconds: float = Field(default=1.0, gt=0)
    backoff_max_seconds: float = Field(default=60.0, gt=0)
    circuit_breaker_threshold: int = Field(
        default=20, ge=1, description="Consecutive infra failures before the run is abandoned."
    )
    circuit_breaker_reset_seconds: float = Field(default=120.0, gt=0)
    preflight_enabled: bool = True
    preflight_timeout_seconds: int = Field(default=10, ge=1)
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
    #: The Trigger Backbone batch-completion topic. Separate from
    #: ``sns_topic_arn``: that one carries failures to RTB, this one carries a
    #: successful business event that TBB starts downstream processing from.
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
    csm: Optional[CSMSettings] = None
    run_marker: RunMarkerSettings = Field(default_factory=RunMarkerSettings)
    audit: AuditSettings = Field(default_factory=AuditSettings)
    resilience: ResilienceSettings = Field(default_factory=ResilienceSettings)
    health: HealthSettings = Field(default_factory=HealthSettings)
    notifications: NotificationSettings = Field(default_factory=NotificationSettings)
    recon: ReconSettings = Field(default_factory=ReconSettings)

    @field_validator("app")
    @classmethod
    def _upper_log_level(cls, v: AppSettings) -> AppSettings:
        v.log_level = v.log_level.upper()
        return v

    @model_validator(mode="after")
    def _cross_checks(self) -> "ConnectorSettings":
        if self.run.mode == "service" and self.source.type == "local":
            raise ValueError("run.mode=service is not supported with source.type=local")
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
        """Whether the upstream reconciliation gate applies to this run.

        Mirrors ``gate_active``: a location that is not configured switches the
        gate off, so a local run needs no recon feed. ``recon.enabled: false``
        is the explicit off-switch for a deployment that has one but wants it
        bypassed.
        """
        if not self.recon.enabled:
            return False
        return bool(self.recon.path or self.recon.trigger_paths.get(self.run.trigger or ""))

    @property
    def gate_active(self) -> bool:
        """Whether the entry-point run gate applies to this configuration.

        Only a batch run with a marker file is gated: a ``service`` run is
        resident and has no invocation to gate, and a local run leaves
        ``run_marker.path`` unset so it needs no trigger and no marker file.
        """
        return self.run.mode == "batch" and bool(self.run_marker.path)

    def select_trigger(self, trigger: Any, *, data_path: Optional[str] = None) -> Optional[str]:
        """Fix this run's trigger and point the source at that trigger's data.

        Precedence for the location: an explicit ``data_path``, then the
        trigger's ``source.trigger_paths`` entry, then ``source.path``.
        """
        if trigger is not None:
            self.run.trigger = canonical_trigger(trigger)

        if data_path:
            self.source.path = str(data_path)
        elif self.run.trigger in self.source.trigger_paths:
            self.source.path = self.source.trigger_paths[self.run.trigger]

        # The recon document lives per trigger too, and is looked up by the same
        # trigger, so the two cannot end up pointing at different triggers.
        if self.run.trigger in self.recon.trigger_paths:
            self.recon.path = self.recon.trigger_paths[self.run.trigger]

        if not self.source.path and not self.run.trigger:
            raise ValueError(
                "No trigger specified, so no source location: set IFC_RUN__TRIGGER "
                "(TRIGGER_8 | TRIGGER_9 | TRIGGER_21)"
            )
        if not self.source.path:
            raise ValueError(
                f"No source location for trigger {self.run.trigger!r}: add it to "
                f"source.trigger_paths or set source.path"
            )
        return self.run.trigger



# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

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


def _env_overlay() -> Dict[str, Any]:
    overlay: Dict[str, Any] = {}
    for key, value in os.environ.items():
        if not key.startswith(_ENV_PREFIX):
            continue
        path = key[len(_ENV_PREFIX) :].lower().split(_SECTION_SEP)
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


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_settings(config_path: str, *, reader=None) -> ConnectorSettings:
    """Read the YAML config, apply the ``IFC_`` env overlay, validate.

    ``reader`` is injected by tests; it defaults to the S3-aware reader.
    """
    if reader is None:
        from ifc_trigger_connector.utility.connector_utility import read_text

        reader = read_text

    document = yaml.safe_load(reader(config_path)) or {}
    if not isinstance(document, dict):
        raise ValueError(f"Config at {config_path} is not a YAML mapping")

    return ConnectorSettings.model_validate(_deep_merge(document, _env_overlay()))