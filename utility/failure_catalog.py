"""The TBB/BSP failure catalogue, encoded.

Every row of the agreed "Failure scenarios to be worked on" matrix appears here
once, with three additions the document does not carry:

* ``handling`` - what this connector does automatically when the scenario
  occurs. ``Handling.NONE`` means the scenario is real but originates outside
  the connector (Databricks, FRED), so all we can do is classify and report.
* ``exit_code`` - the process exit code the connector uses, so an ECS task's
  ``stoppedReason`` / exit code alone tells RTB which scenario fired without
  reading logs.
* ``retryable`` / ``producer_fix_required`` - drives the retry and circuit
  breaker decisions in ``publisher.py``.

Exit codes are grouped: 10-19 infrastructure, 20-29 data/contract,
30-39 platform, 40-49 security. 0 is success, 75 is "drained cleanly but work
remains" (EX_TEMPFAIL), which ECS should treat as a normal restart.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List


class Layer(str, Enum):
    AWS_INFRA = "AWS Infra"
    DATABRICKS_TED = "Databricks / TED layer"
    KAFKA_BSP = "Kafka / BSP Platform Layer"
    SECURITY = "Security"


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    WARNING = "WARNING"
    LOW = "LOW"


class Handling(str, Enum):
    """What the connector does on its own before escalating."""

    #: Detected before any publish; the run refuses to start.
    PREFLIGHT_ABORT = "preflight_abort"
    #: Retried in-process with exponential backoff and jitter.
    RETRY_BACKOFF = "retry_backoff"
    #: The offending record is diverted to S3 quarantine; the run continues.
    QUARANTINE = "quarantine"
    #: SIGTERM-driven drain: stop intake, flush, checkpoint, exit cleanly.
    GRACEFUL_DRAIN = "graceful_drain"
    #: Bounded memory: streaming reads and a capped producer queue.
    BACKPRESSURE = "backpressure"
    #: Counted and reported in the run manifest; no automatic remediation.
    RECONCILE_REPORT = "reconcile_report"
    #: Outside the connector boundary - classify and notify only.
    NONE = "none"


class Owner(str, Enum):
    PRODUCER_RTB = "Producer RTB"
    TBB_RTB = "TBB RTB"
    BSP = "BSP Platform"
    EDP = "EDP / Databricks"


@dataclass(frozen=True)
class Scenario:
    key: str
    layer: Layer
    error_type: str
    error_details: str
    causes: str
    impact: str
    typical_errors: List[str]
    severity: Severity
    handling: Handling
    exit_code: int
    retryable: bool
    producer_fix_required: bool
    incident_owner: Owner
    producer_rtb_action: str
    bsp_rtb_action: str
    connector_behaviour: str
    inform: List[Owner] = field(default_factory=list)

    @property
    def handled_in_connector(self) -> bool:
        return self.handling is not Handling.NONE

    def to_dict(self) -> Dict[str, object]:
        return {
            "key": self.key,
            "layer": self.layer.value,
            "error_type": self.error_type,
            "severity": self.severity.value,
            "handling": self.handling.value,
            "exit_code": self.exit_code,
            "retryable": self.retryable,
            "producer_fix_required": self.producer_fix_required,
            "incident_owner": self.incident_owner.value,
            "inform": [o.value for o in self.inform],
            "connector_behaviour": self.connector_behaviour,
            "producer_rtb_action": self.producer_rtb_action,
            "bsp_rtb_action": self.bsp_rtb_action,
        }


# Sentinel exit codes -------------------------------------------------------

EXIT_OK = 0
EXIT_WORK_REMAINING = 75  # EX_TEMPFAIL: drained on SIGTERM, more work pending.


SCENARIOS: Dict[str, Scenario] = {}


def _register(scenario: Scenario) -> Scenario:
    if scenario.key in SCENARIOS:
        raise ValueError(f"Duplicate scenario key: {scenario.key}")
    SCENARIOS[scenario.key] = scenario
    return scenario


# --------------------------------------------------------------------------
# AWS infrastructure
# --------------------------------------------------------------------------

CONTAINER_FAILURE = _register(
    Scenario(
        key="PRODUCER_CONTAINER_FAILURE",
        layer=Layer.AWS_INFRA,
        error_type="Producer Container Failure",
        error_details=(
            "ECS task/container running the Kafka producer crashes or exits unexpectedly."
        ),
        causes="Application defect; uncaught exception; missing environment variable; dependency issue.",
        impact="No data published while the task is down; backlog may build upstream.",
        typical_errors=["container exited", "application exception", "task stopped", "essential container"],
        severity=Severity.CRITICAL,
        handling=Handling.GRACEFUL_DRAIN,
        exit_code=10,
        retryable=True,
        producer_fix_required=True,
        incident_owner=Owner.PRODUCER_RTB,
        inform=[Owner.TBB_RTB],
        producer_rtb_action="Restart task; review CloudWatch logs, task exit reason and recent deployment changes.",
        bsp_rtb_action="No action unless a BSP-side issue caused repeated producer termination.",
        connector_behaviour=(
            "SIGTERM installs a drain: intake stops, in-flight messages are flushed within "
            "run.shutdown_grace_seconds, the checkpoint is persisted, and the exit code says whether "
            "work remains (75) or the batch completed (0). An uncaught defect still exits non-zero, "
            "but the checkpoint means the restarted task resumes rather than republishes."
        ),
    )
)

OUT_OF_MEMORY = _register(
    Scenario(
        key="PRODUCER_OUT_OF_MEMORY",
        layer=Layer.AWS_INFRA,
        error_type="Producer Out Of Memory",
        error_details="Producer exhausts allocated memory; the container restarts or becomes unhealthy.",
        causes="Large payloads; memory leak; insufficient ECS task memory; excessive batching or retries.",
        impact="Task restarts repeatedly; publishing delayed or stopped.",
        typical_errors=["OutOfMemoryError", "MemoryError", "task killed", "exit code 137", "OOMKilled"],
        severity=Severity.CRITICAL,
        handling=Handling.BACKPRESSURE,
        exit_code=11,
        retryable=True,
        producer_fix_required=True,
        incident_owner=Owner.PRODUCER_RTB,
        inform=[Owner.TBB_RTB],
        producer_rtb_action="Increase task resources if justified, tune batching, investigate payload size and leaks.",
        bsp_rtb_action="No action unless caused by prolonged BSP outage/backlog.",
        connector_behaviour=(
            "Source objects are streamed one at a time, never listed into memory. The librdkafka queue "
            "is capped at kafka.local_queue_max_messages and BufferError applies back-pressure instead "
            "of growing the heap. Container RSS and the RSS/limit ratio are published as CloudWatch "
            "metrics so the alarm fires before exit code 137."
        ),
    )
)

NETWORK_FAILURE = _register(
    Scenario(
        key="NETWORK_CONNECTIVITY_FAILURE",
        layer=Layer.AWS_INFRA,
        error_type="Network Connectivity Failure",
        error_details="AWS producer cannot reach BSP endpoints.",
        causes="Firewall/routing issue; security group or NACL problem; DNS failure; proxy issue.",
        impact="Producer completely disconnected from BSP; no messages published.",
        typical_errors=[
            "connection timeout", "DNS failure", "TLS handshake", "name or service not known",
            "getaddrinfo", "no route to host", "connection refused", "network is unreachable",
        ],
        severity=Severity.CRITICAL,
        handling=Handling.PREFLIGHT_ABORT,
        exit_code=12,
        retryable=True,
        producer_fix_required=False,
        incident_owner=Owner.PRODUCER_RTB,
        inform=[Owner.TBB_RTB, Owner.BSP],
        producer_rtb_action="Validate AWS network path, VPC routing, security groups, DNS and endpoint configuration.",
        bsp_rtb_action="Validate BSP endpoint availability and network reachability from the BSP side.",
        connector_behaviour=(
            "Preflight resolves every bootstrap host and the Schema Registry host and opens a TCP "
            "socket to 9095/8095 before authenticating. A failure names the exact host:port that was "
            "unreachable and which of DNS or TCP failed, which is the evidence the firewall request needs."
        ),
    )
)

BDP_WRITE_FAILURE = _register(
    Scenario(
        key="TRIGGER_BDP_WRITE_FAILURE",
        layer=Layer.AWS_INFRA,
        error_type="Trigger BDP Write Failure",
        error_details="TED is unable to persist trigger events into Trigger BDP/S3.",
        causes="S3/KMS permission issue; bucket policy issue; storage outage; incorrect path.",
        impact="FRED receives no input; trigger events may need rerun.",
        typical_errors=["AccessDenied", "S3 write failure", "path not found", "KMS"],
        severity=Severity.HIGH,
        handling=Handling.NONE,
        exit_code=13,
        retryable=False,
        producer_fix_required=True,
        incident_owner=Owner.PRODUCER_RTB,
        inform=[Owner.TBB_RTB],
        producer_rtb_action="Producer RTB to investigate the S3/KMS permission issue and resolve.",
        bsp_rtb_action="No action.",
        connector_behaviour=(
            "Upstream of the connector. The connector's own writes (manifest, quarantine, audit "
            "payloads) raise SourceAccessError and classify here so an audit-write permission problem "
            "is not mistaken for a Kafka problem."
        ),
    )
)

BDP_READ_FAILURE = _register(
    Scenario(
        key="TRIGGER_BDP_READ_FAILURE",
        layer=Layer.AWS_INFRA,
        error_type="Trigger BDP Read Failure",
        error_details="Unable to read trigger events from Trigger BDP/S3.",
        causes="Missing files; permissions issue; KMS issue; incorrect input path.",
        impact="Event processing halted before action generation.",
        typical_errors=["AccessDenied", "NoSuchKey", "file not found", "invalid path", "NoSuchBucket"],
        severity=Severity.HIGH,
        handling=Handling.PREFLIGHT_ABORT,
        exit_code=14,
        retryable=False,
        producer_fix_required=True,
        incident_owner=Owner.PRODUCER_RTB,
        inform=[Owner.TBB_RTB],
        producer_rtb_action="Producer RTB to investigate; usually environment configuration drift or missing permissions.",
        bsp_rtb_action="No action.",
        connector_behaviour=(
            "Preflight lists the source prefix before authenticating to BSP, so a permission or path "
            "problem is reported in seconds rather than after a full auth handshake."
        ),
    )
)

SCHEMA_VALIDATION_FAILURE = _register(
    Scenario(
        key="SCHEMA_VALIDATION_FAILURE",
        layer=Layer.AWS_INFRA,
        error_type="Schema Validation Failure",
        error_details="Payload is rejected because it does not match the expected schema.",
        causes="Producer schema incompatible with the registered version; mandatory field missing; invalid data type.",
        impact="Publishing blocked for invalid messages; valid messages continue.",
        typical_errors=[
            "SerializationException", "schema compatibility", "invalid payload",
            "ValidationError", "is not an example of the schema",
        ],
        severity=Severity.HIGH,
        handling=Handling.QUARANTINE,
        exit_code=20,
        retryable=False,
        producer_fix_required=True,
        incident_owner=Owner.PRODUCER_RTB,
        inform=[Owner.TBB_RTB],
        producer_rtb_action="Fix payload generation, schema mapping and validation logic. Quarantine failed messages.",
        bsp_rtb_action="Support only if the registry or platform schema configuration changed unexpectedly.",
        connector_behaviour=(
            "Three gates: the IFC payload is checked against the BSP payload JSON Schema, the envelope "
            "against the local .avsc, and the local .avsc against the registered subject at preflight. "
            "A record failing the first two is quarantined to S3 with "
            "the validation error; valid records in the same batch continue. Registry drift found at "
            "preflight aborts the run before anything is published."
        ),
    )
)

FRED_PROCESSING_FAILURE = _register(
    Scenario(
        key="FRED_PROCESSING_FAILURE",
        layer=Layer.AWS_INFRA,
        error_type="FRED Processing Failure",
        error_details="FRED job terminates unexpectedly while resolving actions.",
        causes="Application defect; infrastructure issue; invalid input; missing configuration.",
        impact="Event actions are not generated; the producer receives no actionable input.",
        typical_errors=["job failed", "runtime exception", "dependency failure"],
        severity=Severity.HIGH,
        handling=Handling.NONE,
        exit_code=15,
        retryable=False,
        producer_fix_required=True,
        incident_owner=Owner.PRODUCER_RTB,
        inform=[Owner.TBB_RTB],
        producer_rtb_action="Producer RTB to investigate; TBB owns rerun decision and failed input handling.",
        bsp_rtb_action="No action.",
        connector_behaviour=(
            "Upstream. Visible to the connector only as an empty source, which is reported distinctly "
            "as ZERO_RECORDS rather than as a successful empty run."
        ),
    )
)

FRED_AUDIT_STORE_FAILURE = _register(
    Scenario(
        key="FRED_AUDIT_STORE_FAILURE",
        layer=Layer.AWS_INFRA,
        error_type="FRED Audit Store Failure",
        error_details="Audit/control records for generated actions cannot be persisted.",
        causes="Audit database outage; permission issue; schema change; storage limit.",
        impact="Loss of operational traceability and control evidence even if publish succeeds.",
        typical_errors=[
            "database connection error", "insert failed", "permission denied",
            "ResourceNotFoundException", "ProvisionedThroughputExceeded", "ConditionalCheckFailed",
        ],
        severity=Severity.HIGH,
        handling=Handling.PREFLIGHT_ABORT,
        exit_code=16,
        retryable=True,
        producer_fix_required=True,
        incident_owner=Owner.PRODUCER_RTB,
        inform=[],
        producer_rtb_action="Producer RTB to investigate. Critical from a controls perspective.",
        bsp_rtb_action="No action.",
        connector_behaviour=(
            "The S3 quarantine and the run manifest are the connector's audit record. A rejected "
            "record is recoverable from its quarantine object alone, and a run that cannot write "
            "the manifest reports it rather than claiming a clean delivery."
        ),
    )
)

# --------------------------------------------------------------------------
# Databricks / TED layer
# --------------------------------------------------------------------------

TED_JOB_FAILURE = _register(
    Scenario(
        key="TED_JOB_FAILURE",
        layer=Layer.DATABRICKS_TED,
        error_type="Zero records in TED due to Databricks job failure",
        error_details="Databricks job fails and does not generate trigger events.",
        causes="Job failure; code defect; dependency issue; cluster/configuration issue.",
        impact="No trigger events created; downstream FRED and Kafka path receives no input.",
        typical_errors=["Databricks job failed", "notebook error", "cluster unavailable"],
        severity=Severity.HIGH,
        handling=Handling.RECONCILE_REPORT,
        exit_code=21,
        retryable=False,
        producer_fix_required=False,
        incident_owner=Owner.EDP,
        inform=[Owner.TBB_RTB],
        producer_rtb_action="Raise with the EDP team based on error type and inform TBB RTB.",
        bsp_rtb_action="No action.",
        connector_behaviour=(
            "An empty source on a scheduled run is reported as ZERO_RECORDS with a non-zero exit, not "
            "as success, so a silent upstream failure cannot masquerade as a clean run."
        ),
    )
)

TED_MISSING_SOURCE_DATA = _register(
    Scenario(
        key="TED_MISSING_SOURCE_DATA",
        layer=Layer.DATABRICKS_TED,
        error_type="Zero records in TED due to missing source data",
        error_details="TED runs successfully but required source data is missing or empty.",
        causes="Upstream feed failure; late data; wrong business date; missing partition.",
        impact="No trigger events generated, or incomplete trigger coverage.",
        typical_errors=["source table not found", "zero input records", "missing partition"],
        severity=Severity.MEDIUM,
        handling=Handling.RECONCILE_REPORT,
        exit_code=22,
        retryable=False,
        producer_fix_required=False,
        incident_owner=Owner.EDP,
        inform=[Owner.TBB_RTB],
        producer_rtb_action="Validate source availability and coordinate with upstream data owners.",
        bsp_rtb_action="No action.",
        connector_behaviour=(
            "Same ZERO_RECORDS signal as a job failure. The manifest records the resolved source path "
            "and execution month so the two causes can be told apart without guesswork."
        ),
    )
)

RECONCILIATION_FAILURE = _register(
    Scenario(
        key="PRODUCER_RECONCILIATION_FAILURE",
        layer=Layer.DATABRICKS_TED,
        error_type="Producer Reconciliation Failure",
        error_details="Counts between TED output, FRED actions, producer publishes and consumer processing do not match.",
        causes="Message loss; duplicates; failed retries; partial processing; consumer issue.",
        impact="Regulatory/control risk; inability to prove complete processing.",
        typical_errors=["count mismatch", "missing event id"],
        severity=Severity.HIGH,
        handling=Handling.RECONCILE_REPORT,
        exit_code=23,
        retryable=False,
        producer_fix_required=True,
        incident_owner=Owner.PRODUCER_RTB,
        inform=[Owner.TBB_RTB, Owner.BSP],
        producer_rtb_action="Investigate producer latency, retries and queue depth.",
        bsp_rtb_action="Validate Kafka topic offsets, broker delivery and retention if needed.",
        connector_behaviour=(
            "Every run writes a manifest to S3 with read / parsed / validated / published / acked / "
            "quarantined counts, the partition-offset range per topic and "
            "the run identity. The run fails if read != published + quarantined + parse failures, so "
            "an unexplained gap surfaces at the producer rather than at the consumer."
        ),
    )
)

# --------------------------------------------------------------------------
# Kafka / BSP platform
# --------------------------------------------------------------------------

HIGH_PUBLISH_LATENCY = _register(
    Scenario(
        key="HIGH_KAFKA_PUBLISH_LATENCY",
        layer=Layer.KAFKA_BSP,
        error_type="High Kafka Publish Latency",
        error_details="Broker accepts messages slowly or acknowledgements are delayed.",
        causes="Kafka cluster load; network degradation; large batches; ISR/replication issue.",
        impact="Processing backlog increases; SLA breach risk; delayed consumer availability.",
        typical_errors=["request timeout", "delivery timeout", "high produce latency", "_TIMED_OUT"],
        severity=Severity.MEDIUM,
        handling=Handling.RETRY_BACKOFF,
        exit_code=30,
        retryable=True,
        producer_fix_required=False,
        incident_owner=Owner.PRODUCER_RTB,
        inform=[Owner.TBB_RTB, Owner.BSP],
        producer_rtb_action="Monitor producer latency, retries, queue depth and backlog. Tune producer config if required.",
        bsp_rtb_action="Review cluster health, broker load and partition state.",
        connector_behaviour=(
            "Ack latency percentiles and queue depth are published as metrics each flush interval. "
            "Delivery timeouts are retried with exponential backoff and jitter; sustained timeouts trip "
            "the circuit breaker rather than accumulating an unbounded in-memory queue."
        ),
    )
)

MESSAGE_TOO_LARGE = _register(
    Scenario(
        key="MESSAGE_TOO_LARGE",
        layer=Layer.KAFKA_BSP,
        error_type="Message Too Large",
        error_details="Payload exceeds the Kafka, producer or broker message size limit (800 KB).",
        causes="Unexpected data volume; payload enrichment issue; incorrect serialisation; limit mismatch.",
        impact="Affected messages rejected; publishing continues for smaller messages.",
        typical_errors=["RecordTooLargeException", "MSG_SIZE_TOO_LARGE", "message size exceeds maximum"],
        severity=Severity.MEDIUM,
        handling=Handling.QUARANTINE,
        exit_code=24,
        retryable=False,
        producer_fix_required=True,
        incident_owner=Owner.PRODUCER_RTB,
        inform=[Owner.TBB_RTB],
        producer_rtb_action="Reduce payload size, split the message or fix payload generation; or ask TBB to raise the limit.",
        bsp_rtb_action="Adjust platform limits only if approved and safe.",
        connector_behaviour=(
            "Serialised size is measured before produce() against kafka.max_message_bytes. An oversized "
            "record is quarantined with its measured size and field breakdown, and the run "
            "published, so the broker never sees a record it would reject and the evidence for a limit "
            "increase request is already captured."
        ),
    )
)

PARTITION_LEADER_FAILURE = _register(
    Scenario(
        key="KAFKA_PARTITION_LEADER_FAILURE",
        layer=Layer.KAFKA_BSP,
        error_type="Kafka Partition Leader Failure",
        error_details="Producer cannot publish to one or more partitions due to leader election or partition issues.",
        causes="Broker failover; partition leader unavailable; ISR issue.",
        impact="Partial publishing disruption; increased latency or retries.",
        typical_errors=[
            "NotLeaderForPartition", "LEADER_NOT_AVAILABLE", "leader not available",
            "NOT_ENOUGH_REPLICAS", "UNKNOWN_TOPIC_OR_PART",
        ],
        severity=Severity.MEDIUM,
        handling=Handling.RETRY_BACKOFF,
        exit_code=31,
        retryable=True,
        producer_fix_required=False,
        incident_owner=Owner.TBB_RTB,
        inform=[Owner.BSP],
        producer_rtb_action="Monitor retries and affected partitions; collect producer logs.",
        bsp_rtb_action="Restore partition health and validate leader election/replication.",
        connector_behaviour=(
            "Usually self-healing: librdkafka refreshes metadata and retries with idempotence on, so "
            "ordering and exactly-once-per-partition semantics hold. The connector only escalates once "
            "the circuit breaker threshold of consecutive failures is crossed."
        ),
    )
)

BROKER_UNAVAILABLE = _register(
    Scenario(
        key="BROKER_UNAVAILABLE",
        layer=Layer.KAFKA_BSP,
        error_type="Broker / Infrastructure Failure - Broker Unavailable",
        error_details="Producer cannot establish a connection with Kafka brokers; publishing stops completely.",
        causes="Broker outage or crash; network disruption; BSP broker infrastructure unavailable.",
        impact="Message publishing fails completely.",
        typical_errors=[
            "NetworkException", "DisconnectException", "ALL_BROKERS_DOWN",
            "_TRANSPORT", "broker transport failure", "Connection refused",
        ],
        severity=Severity.CRITICAL,
        handling=Handling.RETRY_BACKOFF,
        exit_code=32,
        retryable=True,
        producer_fix_required=False,
        incident_owner=Owner.TBB_RTB,
        inform=[Owner.BSP],
        producer_rtb_action="Raise a P3 incident with TBB RTB. Capture producer logs, timestamps, affected topic and retry status.",
        bsp_rtb_action="Restore broker availability; validate cluster health and network connectivity.",
        connector_behaviour=(
            "Backoff with jitter up to resilience.backoff_max_seconds. After "
            "resilience.circuit_breaker_threshold consecutive failures the run is abandoned with exit "
            "code 32 and the checkpoint intact, so nothing is lost and the next scheduled run resumes. "
            "In service mode the breaker half-opens after circuit_breaker_reset_seconds."
        ),
    )
)

TOPIC_UNAVAILABLE = _register(
    Scenario(
        key="TOPIC_UNAVAILABLE",
        layer=Layer.KAFKA_BSP,
        error_type="Topic Unavailable / Incorrect Topic",
        error_details="Destination topic does not exist, was deleted or renamed, or the producer points at the wrong topic.",
        causes="Topic not created; incorrect name; wrong environment variable; deployment config mismatch.",
        impact="Messages are rejected, or the producer retries without ever succeeding.",
        typical_errors=["UnknownTopicOrPartitionException", "UNKNOWN_TOPIC", "unknown topic or partition"],
        severity=Severity.HIGH,
        handling=Handling.PREFLIGHT_ABORT,
        exit_code=33,
        retryable=False,
        producer_fix_required=True,
        incident_owner=Owner.TBB_RTB,
        inform=[Owner.BSP],
        producer_rtb_action="Validate producer topic configuration and deployment variables; raise a change request if the topic is missing.",
        bsp_rtb_action="Create/restore the topic or confirm valid topic details where BSP owns provisioning.",
        connector_behaviour=(
            "Preflight fetches cluster metadata for the topic and asserts partition "
            "counts are non-zero. A typo in a task-definition override fails in seconds instead of "
            "retrying forever - the common configuration mistake during POC and early rollout."
        ),
    )
)

SCHEMA_REGISTRY_UNAVAILABLE = _register(
    Scenario(
        key="SCHEMA_REGISTRY_UNAVAILABLE",
        layer=Layer.KAFKA_BSP,
        error_type="Schema Registry Unavailable",
        error_details="Producer cannot retrieve or validate schema metadata before publishing.",
        causes="Registry outage; network issue; DNS problem; firewall issue.",
        impact="Publishing fails, or retries indefinitely if schema lookup is mandatory.",
        typical_errors=["schema registry", "timeout connecting to schema registry", "502 Bad Gateway", "503", "504"],
        severity=Severity.CRITICAL,
        handling=Handling.RETRY_BACKOFF,
        exit_code=34,
        retryable=True,
        producer_fix_required=False,
        incident_owner=Owner.TBB_RTB,
        inform=[Owner.BSP],
        producer_rtb_action="Monitor the retry queue and producer logs; confirm the registry endpoint configuration.",
        bsp_rtb_action="Restore the registry service and validate endpoint availability.",
        connector_behaviour=(
            "The schema id is resolved once at startup and cached for the life of the process, so a "
            "mid-run registry outage does not stop publishing. Lookups retry with backoff; only a "
            "registry that is unavailable at startup blocks the run."
        ),
    )
)

# --------------------------------------------------------------------------
# Security
# --------------------------------------------------------------------------

AUTHENTICATION_FAILURE = _register(
    Scenario(
        key="AUTHENTICATION_FAILURE",
        layer=Layer.SECURITY,
        error_type="Authentication Failure",
        error_details="Producer is unable to authenticate with BSP.",
        causes="Expired BAM certificate; invalid credentials; keystore/truststore issue; incorrect principal.",
        impact="No messages published to Kafka.",
        typical_errors=[
            "SaslAuthenticationException", "SSLHandshakeException", "authentication failed",
            "_AUTHENTICATION", "401", "invalid_grant", "token", "Unauthorized",
        ],
        severity=Severity.CRITICAL,
        handling=Handling.PREFLIGHT_ABORT,
        exit_code=40,
        retryable=False,
        producer_fix_required=True,
        incident_owner=Owner.PRODUCER_RTB,
        inform=[Owner.TBB_RTB],
        producer_rtb_action="Validate BAM credentials, certificate validity, truststore path and producer auth config before escalating.",
        bsp_rtb_action="Support if the authentication infrastructure or BSP-side auth service is unavailable.",
        connector_behaviour=(
            "CyberArk CCP retrieval, BAM token acquisition and a Schema Registry call all happen in preflight, "
            "so an expired certificate or rotated credential fails before any record is read. The JWT "
            "is validated for shape and its exp claim is tracked; the SR token is refreshed ahead of "
            "expiry so long service-mode runs do not fail mid-batch."
        ),
    )
)

AUTHORISATION_FAILURE = _register(
    Scenario(
        key="AUTHORISATION_FAILURE",
        layer=Layer.SECURITY,
        error_type="Authorisation Failure",
        error_details="Producer authenticates but is not authorised to publish to the target topic.",
        causes="Missing ACLs; incorrect principal; topic permission revoked; environment mismatch.",
        impact="Publishing fails for the affected topic while connectivity still appears healthy.",
        typical_errors=[
            "TopicAuthorizationException", "ClusterAuthorizationException",
            "TOPIC_AUTHORIZATION_FAILED", "GROUP_AUTHORIZATION_FAILED", "not authorized", "403",
        ],
        severity=Severity.CRITICAL,
        handling=Handling.PREFLIGHT_ABORT,
        exit_code=41,
        retryable=False,
        producer_fix_required=True,
        incident_owner=Owner.TBB_RTB,
        inform=[Owner.BSP],
        producer_rtb_action="Raise with TBB RTB; check and raise the ACL request; validate producer identity, topic name and environment.",
        bsp_rtb_action="Validate and correct topic ACL configuration where ownership sits with BSP.",
        connector_behaviour=(
            "Metadata for the target topic is requested with the real producer principal at preflight, "
            "so a missing ACL is reported with the principal and topic in the message - typically seen "
            "at onboarding, certificate rotation or service-account change."
        ),
    )
)

UNKNOWN = _register(
    Scenario(
        key="UNKNOWN",
        layer=Layer.AWS_INFRA,
        error_type="Unclassified failure",
        error_details="The error did not match any catalogued scenario.",
        causes="New failure mode, or an error string that has changed upstream.",
        impact="Unknown; treat as a publishing outage until triaged.",
        typical_errors=[],
        severity=Severity.HIGH,
        handling=Handling.NONE,
        exit_code=1,
        retryable=False,
        producer_fix_required=True,
        incident_owner=Owner.PRODUCER_RTB,
        inform=[Owner.TBB_RTB],
        producer_rtb_action="Triage from the captured log context and add a pattern to the classifier.",
        bsp_rtb_action="Support on request.",
        connector_behaviour=(
            "Full error text, Kafka error name and code are captured in the manifest so the pattern can "
            "be added to failures/classifier.py."
        ),
    )
)


def scenarios_by_layer() -> Dict[Layer, List[Scenario]]:
    grouped: Dict[Layer, List[Scenario]] = {layer: [] for layer in Layer}
    for scenario in SCENARIOS.values():
        if scenario.key != "UNKNOWN":
            grouped[scenario.layer].append(scenario)
    return grouped


def get(key: str) -> Scenario:
    return SCENARIOS.get(key, UNKNOWN)
