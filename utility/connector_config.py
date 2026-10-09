from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from utility.trigger_definitions import resolve as resolve_trigger

DEFAULT_MAX_MESSAGE_BYTES = 800 * 1024


class AppSettings(BaseModel):
    name: str = "ifc-trigger-connector"
    environment: str = "UAT"
    log_level: str = "INFO"


DEFAULT_ORIGINATING_SYSTEMS: Dict[str, str] = {
    "DEV": "SNSVC0084379",
    "SIT": "SNSVC0084378",
    "PROD-ANALYTICS": "SNSVC0084375",
    "PROD-PARALLEL": "SNSVC0084371",
    "PROD": "SNSVC0084373",
}

_SYSTEM_CODE = re.compile(r"^[A-Za-z0-9-]+$")


class EnvelopeSettings(BaseModel):
    originating_systems: Dict[str, str] = Field(default_factory=lambda: dict(DEFAULT_ORIGINATING_SYSTEMS))
    tokenised_environments: List[str] = Field(default_factory=lambda: ["PROD"])

    @field_validator("tokenised_environments", mode="before")
    @classmethod
    def _split_a_string(cls, value: Any) -> Any:
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
        bad = {env: code for env, code in systems.items() if not _SYSTEM_CODE.match(code)}
        if bad:
            raise ValueError(f"envelope.originating_systems codes must be letters, digits or '-': {bad}")
        return systems


class RunSettings(BaseModel):
    shutdown_grace_seconds: int = Field(
        default=90,
        ge=5,
        description="Must be <= the ECS task definition stopTimeout, or SIGKILL wins.",
    )
    trigger: Optional[str] = None
    force: bool = False
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
    path: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _no_table(cls, data: Any) -> Any:
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
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return resolve_trigger(value).sub_type
    except KeyError as exc:
        raise ValueError(str(exc)) from None


_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def sql_identifier(value: str, *, what: str) -> str:
    if not isinstance(value, str) or not _SQL_IDENTIFIER.match(value):
        raise ValueError(f"{what} {value!r} is not a plain SQL identifier")
    return f'"{value}"'


def athena_table(name: str) -> str:
    parts = name.split(".") if isinstance(name, str) else []
    if len(parts) != 2:
        raise ValueError(f"Athena source {name!r} must be named as database.table")
    database, table = parts
    return (
        f"{sql_identifier(database, what='Athena database')}."
        f"{sql_identifier(table, what='Athena table')}"
    )


class AthenaSettings(BaseModel):
    workgroup: str = "primary"
    catalog: str = "AwsDataCatalog"
    output_location: Optional[str] = None
    business_date_column: str = "business_date"
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
    model_config = ConfigDict(extra="forbid")

    athena: AthenaSettings = Field(default_factory=AthenaSettings)
    trigger_tables: Dict[str, str] = Field(default_factory=dict)
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
        if not self.table:
            raise ValueError(
                "source.table is not resolved: set IFC_RUN__TRIGGER so the trigger's "
                "source.trigger_tables entry is used"
            )
        return self.table


class ReconSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    table: Optional[str] = None
    trigger_targets: Dict[str, str] = Field(default_factory=dict)
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
    bsp_config_path: Optional[str] = None
    bsp_config_paths: Dict[str, str] = Field(default_factory=dict)
    max_message_bytes: int = Field(default=DEFAULT_MAX_MESSAGE_BYTES, ge=1024)
    flush_timeout_seconds: int = Field(default=120, ge=1)
    local_queue_max_messages: int = Field(default=20000, ge=100)
    overrides: Dict[str, Any] = Field(default_factory=dict)
    debug: Optional[str] = None

    @field_validator("debug")
    @classmethod
    def _no_config_dump(cls, value: Optional[str]) -> Optional[str]:
        contexts = {part.strip().lower() for part in (value or "").split(",") if part.strip()}
        if contexts & {"all", "conf"}:
            raise ValueError("kafka.debug must not include 'all' or 'conf': they print the client config")
        return ",".join(sorted(contexts)) or None


def _join_url_list(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return ",".join(str(item) for item in value)
    return value


class SchemaRegistryModeSettings(BaseModel):
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
    mode: Literal["DEV", "SECURE"] = "SECURE"
    dev: Optional[SchemaRegistryModeSettings] = None
    secure: Optional[SchemaRegistryModeSettings] = None
    url: Optional[str] = None
    ca_location: Optional[str] = None
    timeout_seconds: int = Field(default=30, ge=1)
    token_refresh_margin_seconds: int = Field(default=300, ge=30)
    max_attempts: int = Field(default=5, ge=1)
    schema_path: str = "utility/schema.json"
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
    secret_id: Optional[str] = None
    secret_region: str = "eu-west-1"
    path: str = "/tmp/ifc-certs/CARoot.pem"


CYBERARK_ENV: Dict[str, str] = {
    "CYBERARK_ENABLED": "enabled",
    "CYBERARK_CCP_URL": "base_url",
    "CYBERARK_APP_ID": "app_id",
    "CYBERARK_SAFE": "safe",
    "CYBERARK_ACCOUNT": "object",
}


class CyberArkSettings(BaseModel):
    enabled: bool = True
    base_url: Optional[str] = None
    app_id: Optional[str] = None
    safe: Optional[str] = None
    folder: str = "Root"
    object: Optional[str] = None
    client_cert_secret_id: str
    client_key_secret_id: str
    secret_region: str = "eu-west-1"
    ca_bundle_path: str = "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"
    ssl_verify: bool = True
    request_timeout: int = 30
    max_attempts: int = Field(default=3, ge=1)
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
    bucket: Optional[str] = None
    manifest_prefix: str = "triggerbackbone/ifc/manifests/"
    quarantine_prefix: str = "triggerbackbone/ifc/quarantine/"
    payload_prefix: str = "triggerbackbone/ifc/payloads/"
    write_payloads: bool = False

    @field_validator("bucket", mode="before")
    @classmethod
    def _strip(cls, v: Any) -> Any:
        return v.strip().rstrip("/") if isinstance(v, str) else v

    @property
    def root(self) -> str:
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
    preflight_timeout_seconds: int = Field(default=30, ge=1)
    preflight_metadata_enabled: bool = False
    preflight_metadata_timeout_seconds: int = Field(default=60, ge=1)
    max_quarantine_ratio: float = Field(default=0.05, ge=0.0, le=1.0)


class HealthSettings(BaseModel):
    enabled: bool = True
    port: int = Field(default=8080, ge=1, le=65535)
    bind_host: str = "0.0.0.0"


class NotificationSettings(BaseModel):
    sns_topic_arn: Optional[str] = None
    application_label: str = "IFC Trigger Connector"
    batch_sns_topic_arn: Optional[str] = None
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
        if not self.recon.enabled:
            return False
        target = self.recon.target_table or self.recon.trigger_targets.get(self.run.trigger or "")
        return bool(self.recon.table and target)

    @property
    def originating_system(self) -> str:
        environment = self.app.environment.strip().upper()
        try:
            return self.envelope.originating_systems[environment]
        except KeyError:
            raise ValueError(
                f"app.environment {self.app.environment!r} has no envelope.originating_systems "
                f"entry, so triggerOriginatingSystem cannot be set; known: "
                f"{sorted(self.envelope.originating_systems)}"
            ) from None

    @property
    def declares_encryption_policies(self) -> bool:
        return self.app.environment.strip().upper() in self.envelope.tokenised_environments

    @property
    def gate_active(self) -> bool:
        return bool(self.run_marker.path)

    @property
    def trigger(self) -> str:
        if not self.run.trigger:
            raise ValueError(
                "No trigger specified: set IFC_RUN__TRIGGER (TRIGGER_8 | TRIGGER_9 | TRIGGER_21)"
            )
        return self.run.trigger

    def select_trigger(self, trigger: Any) -> str:
        if trigger is not None:
            self.run.trigger = canonical_trigger(trigger)
        selected = self.trigger

        if selected in self.source.trigger_tables:
            self.source.table = self.source.trigger_tables[selected]

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
    lowered = raw.strip().lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none", ""}:
        return None
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return raw


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
                nxt = {}
                cursor[part] = nxt
            cursor = nxt
        cursor[path[-1]] = _coerce(value)
    return overlay


def _cyberark_env_overlay() -> Dict[str, Any]:
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


ENVIRONMENT_VARIABLE = "IFC_APP__ENVIRONMENT"

ENVIRONMENT_CONFIGS = {
    "DEV": "utility/connector_config_dev.yaml",
    "SIT": "utility/connector_config_sit.yaml",
    "PROD": "utility/connector_config_prod.yaml",
}


def config_path_for(environment: Optional[str]) -> str:
    from utility.connector_utility import package_resource

    name = (environment or "").strip().upper()
    if not name:
        raise ValueError(f"{ENVIRONMENT_VARIABLE} is not set; expected one of {', '.join(ENVIRONMENT_CONFIGS)}")
    if name not in ENVIRONMENT_CONFIGS:
        raise ValueError(
            f"{ENVIRONMENT_VARIABLE}={environment!r} has no connector config; "
            f"expected one of {', '.join(ENVIRONMENT_CONFIGS)}"
        )

    path = package_resource(ENVIRONMENT_CONFIGS[name])
    if not os.path.isfile(path):
        raise FileNotFoundError(f"{ENVIRONMENT_VARIABLE}={name} needs {ENVIRONMENT_CONFIGS[name]}, which is missing")
    return path


def load_settings(config_path: str, *, reader=None) -> ConnectorSettings:
    if reader is None:
        from utility.connector_utility import read_text

        reader = read_text

    document = yaml.safe_load(reader(config_path)) or {}
    if not isinstance(document, dict):
        raise ValueError(f"Config at {config_path} is not a YAML mapping")

    layered = _deep_merge(_deep_merge(document, _cyberark_env_overlay()), _env_overlay())
    _select_bsp_config(layered)
    settings = ConnectorSettings.model_validate(layered)

    from utility.run_gate import set_execution_month

    set_execution_month(settings.run.month)
    return settings