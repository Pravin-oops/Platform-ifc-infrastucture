# IFC Trigger Connector

A custom Kafka connector that runs on **AWS ECS** and publishes **IFC CDD trigger events
(Triggers 8, 9 and 21)** from the Trigger Event BDP onto the **BSP on-prem Trigger Backbone**.

It runs once a month against one extract per trigger, and every design decision below follows
from that: a long-lived container reading a single large file, on a schedule where nobody is
watching the logs, has to leave evidence rather than assume someone saw it fail.

---

## Contents

1. [Scope](#scope)
2. [Design constraints](#design-constraints)
3. [Architecture](#architecture) · [Repository layout](#repository-layout)
4. [Failure scenario coverage](#failure-scenario-coverage)
5. [Message contracts](#message-contracts)
6. [The IFC trigger payloads](#the-ifc-trigger-payloads)
7. [Identity, idempotency and ordering](#identity-idempotency-and-ordering)
8. [Reconciliation and audit](#reconciliation-and-audit)
9. [Running it](#running-it)
10. [Configuration](#configuration)
11. [The upstream reconciliation gate](#the-upstream-reconciliation-gate) · [The invocation gate](#the-invocation-gate)
12. [Exit codes](#exit-codes)
13. [Deployment](#deployment)
14. [The batch completion notification](#the-batch-completion-notification)
15. [Contract drift found in the source documents](#contract-drift-found-in-the-source-documents)
16. [Open items before UAT](#open-items-before-uat)

---

## Scope

**In scope.** Reading detected trigger events from the Trigger BDP; building the
`TriggerBackboneTopicSchema` envelope and the BSP-format payload for Triggers 8, 9 and 21;
authenticating to BSP through CSM and BAM; validating against the BSP Schema Registry;
publishing to the IFC topic; and handling every failure scenario that can be handled
at the connector.

**Not in scope.** Detection itself (TED, on Databricks), action resolution (FRED), and the
consumer side. Failures in those layers are *classified and reported* by this connector — with
the agreed ownership and actions — but not remediated by it. The catalogue is explicit about
which is which.

---

## Design constraints

Five behaviours follow from running a monthly batch in a container rather than a short-lived
function, and each is a deliberate choice rather than an incidental one.

| Constraint | What the connector does | Why |
|---|---|---|
| **The batch is one large file** | Objects are streamed and the producer queue is capped; `BufferError` applies back-pressure instead of growing the heap | Loading a month's records eagerly is how a container gets OOM-killed (exit 137) |
| **ECS stops tasks on every deployment** | `SIGTERM` stops intake, flushes what is in flight within the grace window, exits 75 | Without a drain, in-flight messages are lost on a routine scale-in |
| **A record's identity has to be reproducible** | `triggerID` is the business-month timestamp plus the record's position in the batch, not the wall clock | A re-run of the same file reproduces the same IDs, so the consumer can recognise a republished record |
| **The task outlives its BAM token** | `exp` is tracked and the token refreshed at a margin | A month's batch must not fail halfway through on an expiry |
| **The container is gone once it exits** | Classify, quarantine, reconcile, write the manifest, exit with a catalogue code | The evidence has to be written while the run is alive; afterwards there is only the stopped-task record |

Two further choices are about proof rather than mechanics: an oversized message is caught
**before** `produce()` so it can be diagnosed rather than merely rejected, and a run whose
numbers do not balance **fails**, even when every individual publish succeeded.

---

## Architecture

```
On-prem sources ─► EDP hydration ─► FDPs / CDPs + SDH
                                          │
                                          ▼
                         TED (Databricks)  ── detects Triggers 8, 9, 21
                                          │
                                          ▼
                      Trigger Event BDP (S3 / Iceberg)
                                          │
                                          ▼
                         FRED (ECS) ── resolves event actions
                                          │
                                          ▼
        ┌──────────────────── THIS CONNECTOR (ECS) ────────────────────┐
        │  recon gate ─► run gate ─► preflight ─► stream ─►            │
        │  build envelope ─► validate ─► serialise ─► size guard ─►    │
        │  publish ─► flush ─► reconcile ─► notify                     │
        │        │                        │                            │
        │        ▼                        ▼                            │
        │  S3 run-marker JSON       S3 quarantine + manifest           │
        └──────────────┬─────────────────────────┬─────────────────────┘
                       │ CSM → BAM → JWT         │
                       ▼                         ▼
              BSP Schema Registry :8095    BSP Kafka :9095
                                                 │
                                                 ▼
                                    IFC Trigger Backbone topic
```

### Repository layout

Executable entry points in `scripts/`, and a **flat** `utility/` holding every module, config,
schema and `requirements.txt` they import. There are no subpackages, no `src/` and no separate
`config/` or `schemas/` trees: a module is either something you run or something the things you
run import, and the directory says which.

```
ifc_trigger_connector/            # imports are ifc_trigger_connector.utility.*
|
├── .env                          # developer env values (git-ignored in real deploys)
├── Docker/Dockerfile             # two-stage image; ENTRYPOINT is scripts/main_ecs.py
|
├── scripts/                      # Executable entry points
│   ├── main.py                   # CLI: run, validate, catalogue
│   ├── main_ecs.py               # ECS platform entry: APP_CONFIG_PATH -> exit code
│   └── main_local.py             # Local developer run, with --dry-run
|
├── utility/
│   ├── __init__.py
│   ├── connector_utility.py      # S3/local path handling, loaders, resource resolution
│   ├── connector_config.py       # typed config; YAML + IFC_ env overlay
│   ├── connector_runner.py       # the batch/service loop
│   ├── trigger_source.py         # streaming Trigger BDP reader
│   ├── recon_gate.py             # upstream reconciliation gate
│   ├── run_gate.py               # weekend / already-delivered gate, run markers
│   ├── sequence_allocator.py     # batch sequence numbers
│   ├── audit_utility.py          # quarantine, run manifest, reconciliation
│   ├── health_utility.py         # /health/live, /ready, /startup, /metrics
│   ├── resilience_utility.py     # backoff, circuit breaker, SIGTERM drain
│   ├── observability_utility.py  # JSON logs, CloudWatch EMF metrics
│   ├── csm_aws_fetch.py          # SigV4 -> Vault -> system-account credential
│   ├── auth_helper.py            # BSP client wrapper, BAM token lifecycle
│   ├── kafka_factory.py          # assembles the stack, runs preflight
│   ├── kafka_preflight.py        # DNS, TCP, auth, registry, metadata checks
│   ├── kafka_serializers.py      # Confluent wire format + size guard
│   ├── kafka_publisher.py        # produce, back-pressure, delivery reports, offsets
│   ├── schema_registry_client.py # registry client + local-vs-registered drift check
│   ├── trigger_payload.py        # the BSP payload contract
│   ├── trigger_definitions.py    # field specs for Triggers 8, 9, 21
│   ├── tb_outcome_schema.py      # envelope construction and identity
│   ├── failure_catalog.py        # the agreed failure matrix, encoded
│   ├── error_classifier.py       # error -> scenario
│   ├── failure_notifier.py       # RTB-ready alert subject and body
│   ├── trigger_batch_notifier.py # TBB batch-completion SNS event
│   |
│   ├── schema.json                      # TriggerBackboneTopicSchema (Avro)
│   ├── connector_config.yaml            # deployed app config (ECS, CSM-backed)
│   ├── connector_config_ecs_local.yaml  # ECS entry point against a local data folder
│   ├── connector_config_local.yaml      # local app config (offline, DEV, localhost broker)
│   ├── connector_config_local_bsp.yaml  # local app config against real BSP (BAM, SECURE)
│   ├── bsp_config_local.yaml            # BSP client librdkafka YAML for a local run
│   └── requirements.txt
|
├── data/                         # local stand-in for the Trigger BDP, per trigger
├── samples/                      # worked examples, used by --dry-run
└── deploy/                       # ECS task definition, IAM, infrastructure, alarms
```

The import root `ifc_trigger_connector` matches the directory on disk, so the app runs from a
checkout with nothing on `PYTHONPATH` beyond the repository root — the directory *above*
`ifc_trigger_connector/`. Each entry point adds that itself, so `python scripts/main_ecs.py`
works from the connector directory without any further setup.

---

## Failure scenario coverage

Every row of the agreed *Failure scenarios to be worked on* matrix is encoded in
[`utility/failure_catalog.py`](utility/failure_catalog.py) with the owning team, the agreed
action, an exit code, and what the connector does automatically.

`python scripts/main.py catalogue` prints the whole thing as JSON.

### Handled at the connector

| Scenario | Handling | What the connector does |
|---|---|---|
| **Producer Container Failure** | graceful drain | SIGTERM stops intake, flushes in-flight messages within the grace window, persists the checkpoint, exits 75 so a restart resumes rather than republishes |
| **Producer Out Of Memory** | back-pressure | Streaming reads (never a full listing in memory), capped producer queue, `BufferError` waits instead of growing the heap; RSS and RSS/limit published so the alarm precedes exit 137 |
| **Network Connectivity Failure** | preflight abort | DNS resolution and a TCP connect to every broker (9095) and the registry (8095) before authenticating — the exact evidence a firewall request needs |
| **Trigger BDP Read Failure** | preflight abort | Source prefix listed before any BSP handshake, so a permission or path error is reported in seconds |
| **Schema Validation Failure** | quarantine | Three gates — payload vs BSP JSON Schema, envelope vs local `.avsc`, local `.avsc` vs registered subject. Bad records go to S3 quarantine; the batch continues; registry drift aborts at startup |
| **FRED Audit Store Failure** | reconcile + report | The S3 quarantine and the run manifest are the audit record; a rejected record is recoverable from the quarantine object alone |
| **Zero records (TED job failure / missing source data)** | reconcile + report | An empty source on a scheduled run exits non-zero as `ZERO_RECORDS`, so a silent upstream failure cannot look like a clean run |
| **Producer Reconciliation Failure** | reconcile + report | Manifest with full counts, per-partition offset ranges, and a balance check that fails the run when it does not hold |
| **High Kafka Publish Latency** | retry + backoff | Ack-latency and queue-depth metrics; delivery timeouts retried with jittered backoff; sustained failure trips the breaker rather than queuing unboundedly |
| **Message Too Large** | quarantine | Serialised size measured against the 800 KB limit *before* `produce()`; oversized records quarantined with a payload/overhead breakdown |
| **Kafka Partition Leader Failure** | retry + backoff | Idempotent producer + librdkafka metadata refresh; escalates only past the breaker threshold |
| **Broker Unavailable** | retry + backoff | Jittered backoff; after N consecutive failures the run is abandoned cleanly with the checkpoint intact |
| **Topic Unavailable / Incorrect Topic** | preflight abort | Cluster metadata for the topic, with a partition-count assertion — a typo fails in seconds |
| **Schema Registry Unavailable** | retry + backoff | Schema id resolved once and cached, so a mid-run outage does not stop publishing |
| **Authentication Failure** | preflight abort | CSM, BAM and a registry call all happen before any record is read; JWT shape checked and `exp` tracked with pre-emptive refresh |
| **Authorisation Failure** | preflight abort | Metadata requested with the real producer principal, so a missing ACL is distinguishable from a missing topic |

### Classified and reported, not remediated

`Trigger BDP Write Failure`, `FRED Processing Failure`, `TED Job Failure` and
`TED Missing Source Data` originate upstream. The connector classifies them, routes the incident
to the right team, and — where they are visible as an empty source — refuses to report success.
It does not pretend to fix them.

---

## Message contracts

Two Avro schemas, transcribed from the *Trigger Backbone : Integration with BSP Kafka topic
details* document, bundled in [`utility/`](utility):

| File | Record | Role here |
|---|---|---|
| `schema.json` | `TriggerBackboneTopicSchema` | **Produced** — the trigger event |

The BSP Schema Registry remains the source of truth. The bundled `.avsc` is what records are
*written with*, so at startup the connector compares it against the registered subject and
refuses to run on a blocking difference — a field the registry does not know about, a mandatory
registry field we do not populate, or a type change. Additive optional fields are logged and
allowed.

Records are written in **Confluent wire format**: `0x00` + 4-byte schema id + Avro body, in every
mode. That is the format `io.confluent.kafka.serializers.KafkaAvroDeserializer` (the consumer's
Flink job) and `confluent_kafka.schema_registry.avro.AvroSerializer` both use; the connector's
own serializer writes the same bytes, but returns them before `produce()` so the size guard can
see them. A record whose first byte is not `0x00` was written without the header.

### The payload field

`payload` is a *JSON string*, not an object. Its content follows the BSP payload contract and
is the field array itself, with no `{"payload": ...}` wrapper around it:

```json
[{"fieldName": "...", "fieldValue": "...",
  "fieldEncryptionPolicy": "...", "fieldDataType": "..."}]
```

Two details that bite:

- `fieldValue` has `minLength: 1`. An optional field with no value must be **omitted**, not sent
  empty. `build_fields` drops them; the validator rejects an empty one explicitly.
- `fieldEncryptionPolicy` is mandatory but may be `""`. Only `Client Relationship Owner Name`
  carries a policy (`DPASS_POLICY_NAME`), and the
  connector never de-tokenises — per the POC RAIDD assumption, values arrive tokenised and the
  connector declares which policy was applied upstream.

---

## The IFC trigger payloads

Field sets live in [`utility/trigger_definitions.py`](utility/trigger_definitions.py) as
declarative `FieldSpec` lists. Each spec separates the **published name** (consumer-visible;
changing it needs the BSP schema change process) from the **source key** (what TED/FRED writes;
changing it is an internal remap).

Every trigger publishes the **same eight fields**, in this order:

| # | `fieldName` | `fieldDataType` | Source key | Policy |
|---|---|---|---|---|
| 1 | `Date of Request` | `DATE` | `date_of_request` | — |
| 2 | `Counterparty Full Legal Entity Name` | `STRING` | `counterparty_full_legal_entity_name` | — |
| 3 | `Counterparty ID` | `STRING` | `counterparty_csid_sds` | — |
| 4 | `Client Relationship Owner Name` | `STRING` | `client_relationship_owner_name` | `DPASS_POLICY_NAME` |
| 5 | `Client Relationship Owner BRID` | `STRING` | `client_relationship_owner_brid` | — |
| 6 | `Client Relationship Owner Business Unit` | `STRING` | `client_relationship_owner_business_unit` | — |
| 7 | `Client Relationship Owner Location` | `STRING` | `client_relationship_owner_location` | — |
| 8 | `Region` | `STRING` | `region` | — |

`Date of Request` is the date the trigger file was generated. Source rows carry a full timestamp;
`DataType.DATE` renders the date part only (`2026-06-10T02:15:04.221Z` → `2026-06-10`). Every other
field is a string taken from the source row unchanged.

`Counterparty ID` is the counterparty's CSID SDS value. It also travels as the envelope's
`idValue`.

`DPASS_POLICY_NAME` goes on `Client Relationship Owner Name` and on **nothing else** — every other
field ships with an empty `fieldEncryptionPolicy`.

The three definitions in `trigger_definitions.py` therefore share one `_payload_fields()` list. A
new trigger joins the topic by reusing it, not by declaring its own field set.

**What is no longer published.** `Trigger_subType_detail`, `Customer Segment`, `Business Date`,
`Last Run Date` and the whole Trigger 21 alert-volume block have been removed from the payload.
The trigger a message belongs to is already carried by the envelope's `triggerSubType`, so the
payload does not repeat it. Source rows may still carry those columns — `business_date` in
particular is still read, as the sub-event discriminator — they simply do not go on the wire.

Because Trigger 21 now publishes the same header columns as Triggers 8 and 9, its definition no
longer depends on a BDP table that has not been built: nothing in it is a guess at an
alert-volume column.

**All eight fields are mandatory.** A source row missing — or blank in — any of the eight is
quarantined with a `SCHEMA_VALIDATION_FAILURE`, naming the field and the source column; it is
never published with a hole, and never sent as an empty `fieldValue`.

Two of the eight carry a contract default, applied when the source column is absent or blank:

| Field | Source column | Default |
| --- | --- | --- |
| Client Relationship Owner Business Unit | `client_relationship_owner_business_unit` | `UK Corporate` |
| Client Relationship Owner Location | `client_relationship_owner_location` | `UK` |

The defaults live in `utility/trigger_definitions.py` (`DEFAULT_BUSINESS_UNIT`,
`DEFAULT_LOCATION`). The other six have to arrive on the row.

**Field lengths.** Each field carries the consumer's column width, from the data-length column of
the SIEBEL consumption table. The length is measured on the *rendered* value — what actually goes
on the wire — so a `date_of_request` arriving as a full timestamp is measured as the ten
characters it renders to, not as the source string:

| Field | Source column | Type | Max length |
| --- | --- | --- | --- |
| Date of Request | `date_of_request` | Date (`YYYY-MM-DD`) | 10 |
| Counterparty Full Legal Entity Name | `counterparty_full_legal_entity_name` | String | 100 |
| Counterparty ID | `counterparty_csid_sds` | String | 11 |
| Client Relationship Owner Name | `client_relationship_owner_name` | String | 50 |
| Client Relationship Owner BRID | `client_relationship_owner_brid` | String | 10 |
| Client Relationship Owner Business Unit | `client_relationship_owner_business_unit` | String | 20 |
| Client Relationship Owner Location | `client_relationship_owner_location` | String | 5 |
| Region | `region` | String | 50 |

The widths live in `MAX_LENGTHS` in `utility/trigger_definitions.py`; changing one is a
consumer-visible change, like changing a `FieldSpec.name`. An over-long value is **rejected, not
truncated** — a clipped CSID or BRID is a different identifier, not a cosmetic difference — and
the quarantine record names the field, the source column, the actual length and the limit.

Two consequences worth knowing:

- **Location is five characters, and that is correct** — the column is a country, not a city. It
  fits `UK` and a country code; it does not fit `London` or `Birmingham`. Every bundled sample
  carries `UK`. If the extract ever sends a city name, those rows are quarantined by design;
  the fix is upstream, not a wider column here.
- **Relationship Owner Name is fifty characters and is tokenised.** The limit is measured on the
  tokenised value the connector receives, not on the cleartext name. If DPASS tokens run longer
  than the cleartext they replace, this width needs re-confirming.

### Input contract

**ECS is handed one file holding the whole batch**, as a bare JSON array, written into a folder
per month:

```
s3://.../trigger-events/trigger_8/SEPTEMBER_2026/trigger8_20260930_143022_123456.json
```

**The folder is resolved at run time.** `source.trigger_paths` carries a template ending
`{MONTH}_{YYYY}`, expanded from the run date, so the config does not get republished every month:

| Token | Renders |
|---|---|
| `{MONTH}` / `{Month}` / `{month}` | `SEPTEMBER` / `September` / `september` |
| `{MON}` | `SEP` |
| `{YYYY}` · `{YY}` · `{MM}` · `{DD}` | `2026` · `26` · `09` · `30` |
| `{YYYYMM}` · `{YYYY-MM}` · `{YYYYMMDD}` | `202609` · `2026-09` · `20260930` |

The date is the **run** month in `Europe/London` — the month the run gate keys on — not the
business month the records describe. A run in September 2026 reads `SEPTEMBER_2026` and publishes
records stamped `2026-08-31T23:59:59.999999999Z`. Expansion happens on each read rather than once
at load, so a resident service crossing a month boundary moves to the new folder without a
restart, and the startup log and run summary both carry the resolved folder alongside the
template.

**Reprocessing a past month.** Set `IFC_RUN__MONTH` (`run.month`) on a one-off RunTask to run as
that month instead of the current one. It takes `2026-08` or the folder's own `AUGUST_2026`. An
October run with `IFC_RUN__MONTH=2026-08` behaves exactly as the August run did: it reads
`AUGUST_2026` (source and recon), stamps records `2026-07-31T23:59:59.999999999Z`, and records its
outcome against `2026-08` in the run marker. The day-only tokens (`{DD}`, `{YYYYMMDD}`) render the
1st. Two things stay on the real date: the weekend check, and the marker line's `run_date`. A
month already marked `SUCCESS` is still skipped unless `IFC_RUN__FORCE=true` is set too, so the
override alone cannot republish a delivered month by accident. Leave it unset on the schedule.

**Only the newest extract in that folder is read.** TED rewrites the month's extract rather than
appending, so an older file beside it is a superseded draft; reading them all would republish
stale content under fresh trigger IDs. Files are ordered by the timestamp *parsed* out of the
name (`source.filename_timestamp_pattern` / `_format`), never by the name itself — a day-first or
month-first convention does not sort lexicographically, and a name sort would quietly select a
month-old extract. A file whose name carries no parseable timestamp loses to any file that has
one, and is logged. `source.selection: all` restores reading everything.

A missing month folder behaves differently by backend: on S3 it lists empty and the run reports
`ZERO_RECORDS`; locally the path must exist or the read raises.

Pointing `source.path` at a single object, or at a prefix with no tokens, both still work —
`iter_object_paths` reads a single key directly without listing — so the change is reversible.

Three layouts are accepted, because upstream writers differ: one JSON object (a single event), a
JSON array (many), or JSON Lines. A wrapper such as `{"records": [...]}` is **not** unwrapped —
it is read as one malformed event.

Two consequences of consolidating into one object, both deliberate:

- **No record cap.** `source.max_records_per_batch` is unset in the deployed config. A cap only
  makes sense when the remainder can be picked up from a later object; with one file it would
  stop mid-array and silently drop the tail. Setting one is a developer convenience for bounding
  a local run, and hitting it now logs a warning saying what was left unpublished.
- **The file is read in one piece.** `read_text` decodes the whole object and `json.loads` builds
  the whole list, so the task needs memory for roughly twice the file size. The decoded text is
  released as soon as the array is parsed, and events are yielded one at a time from there. If the
  batch ever outgrows the task, `.jsonl` from upstream is the fix — it also degrades per record
  instead of losing the whole batch to one malformed byte.

A source that **cannot be read** — a missing key, `AccessDenied` — fails the run as
`SOURCE_UNREADABLE` with the `TRIGGER_BDP_READ_FAILURE` exit code. It reaches the runner as a
single parse failure, which on its own reconciles cleanly, so without that rule a missing monthly
file would exit 0 and look like a clean run. An **empty** array is different and stays
`ZERO_RECORDS`: the file arrived, and held nothing.

The wrapper names the trigger; `attributes` carries the BDP row verbatim. Upstream supplies no
customer id, business unit or timestamp — the connector derives them:

```json
{"triggerType": "IFC_CDD", "triggerSubType": "TRIGGER_8",
 "attributes": {"counterparty_csid_sds": 9912345678, "business_date": "2026-06-30", "...": "..."}}
```

`triggerSubType` is required; `triggerType` and `upstreamTriggerId` are optional; any other
top-level key is ignored.

| Envelope field | Value |
|---|---|
| `triggerID` (and the Kafka key) | `{system}_{triggerType}_{triggerSubType}_{timestamp}_{sequenceNumber}` — see below |
| `triggerType` | fixed: `KYCRefresh` (the event's own `triggerType` is ignored) |
| `triggerSubType` | the published enum symbol for the trigger |
| `timestamp` | last instant of the **business month**, the month before the run date (UK time): a July 2026 run stamps every record `2026-06-30T23:59:59.999999999Z` |
| `triggerPostingTimestamp` | when the record is posted, same RFC 3339 format (UTC, nanosecond precision) |
| `sequenceNumber` | the record's position in the batch — 1, 2, 3… across all customers |
| `triggerOriginatingSystem` | fixed: `SNSVC0084378` |
| `triggerOriginatingBU` | fixed: `UK-C` |
| `idSystem` | fixed: `Corelation id` |
| `idType` | fixed: `Customer` |
| `idValue` | `counterparty_csid_sds`, as a string; a row without one is quarantined |
| `upstreamTriggerID` | the event's `upstreamTriggerId`, usually null |
| `payload` | the eight contract fields above |

The sub-event discriminator is the row's `business_date`. Neither table has a sub-event column,
and the grain is one row per counterparty per business date, so without it a second row for one
counterparty inside a business month would collide with the first on trigger ID.

Worked examples: [`samples/trigger_events.jsonl`](samples/trigger_events.jsonl)
and the message it produces,
[`samples/expected/trigger_8_message.json`](samples/expected/trigger_8_message.json).

---

## Identity, idempotency and ordering

**Trigger ID** follows the Trigger Backbone's format:

```
{system}_{triggerType}_{triggerSubType}_{timestamp}_{sequenceNumber}
SNSVC0084378_KYCRefresh_NewHRCRelationship_2026-06-30T23:59:59.999999999Z_1
```

`timestamp` is the envelope's business-month stamp, the same for every record in a run, so the
**sequence number is what makes the ID unique**. `sequenceNumber` is the record's position in the
batch — **1, 2, 3… across all customers**, the way the reference producer numbers its batch:

```
csid=9912345678  seq=1  triggerID=…_2026-06-30T23:59:59.999999999Z_1
csid=9912345678  seq=2  triggerID=…_2026-06-30T23:59:59.999999999Z_2
csid=9912345679  seq=3  triggerID=…_2026-06-30T23:59:59.999999999Z_3
```

It is allocated in process for the life of the run: one monthly batch is one file read by one run,
and the file is read in order, so a re-run of the same file reproduces the same numbers and IDs.
A re-run of a *changed* file can shift them. **The connector does not de-duplicate** — it keeps no
durable record of what it sent, and a re-run republishes the month in full.

The business month comes from the **run date**, not the data, so the same rows processed in a
different calendar month get a different month, timestamp and trigger ID. A late re-delivery of
June has to run in July; there is no config override for the month yet.

**The Kafka key is the trigger ID**, as in the Trigger Backbone's reference records. It is unique
per record, so records spread across partitions and a counterparty's events are not guaranteed to
stay in order relative to each other. Records carry **no Kafka headers**.

Each built record still carries a **business key** — canonical sorted-key JSON of system, trigger
type, sub-type, business month, `idType`, the CSID, the sub-event discriminator (`business_date`
for Triggers 8 and 9) and, from a customer's second event onwards, that customer's occurrence
number. It is not published and no longer feeds the trigger ID.

Only **acknowledged** messages are counted as published. An unacknowledged message is left for the
next run rather than silently dropped.

---

## Reconciliation and audit

`audit.bucket` takes either a bare bucket name or a full `s3://bucket/folder` URI, for evidence
that lives under a folder rather than at the root of a bucket of its own. The three prefixes are
appended to it, so a URI naming a folder nests them under that folder. Every run writes a manifest
to `<audit.bucket>/<manifest_prefix>/run_date=…/<run_id>.json`
containing the run and task identity, configuration in force, full counters, per-partition
offset ranges, the preflight report, the source objects read, quarantine object keys, and the
classified failure if there was one.

The control is a balance identity:

```
records read = published + quarantined + parse failures
```

If it does not hold — or if published ≠ acknowledged, or the flush timed out with messages still
queued — the run **fails** with `PRODUCER_RECONCILIATION_FAILURE`, even when every individual
publish succeeded. An unexplained gap is exactly what the control exists to catch.

A separate quality gate fails the run when the quarantined fraction exceeds
`resilience.max_quarantine_ratio` (default 5%), so a run cannot publish a fraction of its content
and report success.

---

## Running it

### Offline contract check — no AWS, no BSP

The fastest way to check a trigger definition change. Run it in CI whenever the definitions or
sample data change:

```bash
python scripts/main.py validate --input samples/trigger_events.jsonl --show-payload
```

```
OK          SNSVC0084378_KYCRefresh_NewHRCRelationship_2026-06-30T23:59:59.999999999Z_1  seq=1
OK          SNSVC0084378_KYCRefresh_NewHRCRelationship_2026-06-30T23:59:59.999999999Z_2  seq=2
OK          SNSVC0084378_KYCRefresh_AccountInactivity_2026-06-30T23:59:59.999999999Z_3  seq=3
OK          SNSVC0084378_KYCRefresh_MultipleTMSARs_2026-06-30T23:59:59.999999999Z_4  seq=4

4 valid, 0 rejected
```

### The failure catalogue

```bash
python scripts/main.py catalogue
```

### Local dry run — no broker, no BSP, no network

```bash
python scripts/main_local.py --dry-run
python scripts/main_local.py --dry-run --input samples/trigger_events.jsonl --show-payload
```

Envelopes are built with the same `EnvelopeBuilder` the runner uses, Avro-validated and
size-checked, then counted. A record that passes here is a record the real pipeline would accept;
the only thing skipped is the network.

### Local run against BSP

`scripts/main_local.py` without `--dry-run` publishes real records: authenticate to BSP Kafka
through `BSPAuthenticator`, fetch a **BAM token over REST** for the secure Schema Registry,
resolve the subject's schema id, then build, serialise and publish, flush, and report the
delivery tally.

There is no CSM on this path: a laptop has no ECS task role, so the BSP system-account
credentials come from the environment. `utility/kafka_factory.py` fetches the same values from
CSM on the ECS path instead.

```bash
set -a && . .env && set +a
export BSP_USERNAME='snsvc...@INTRANET.BARCAPINT.COM' BSP_PASSWORD='...'
export BAM_URL='https://bamuat-auth.client.barclayscorp.com/authn/authenticate/sso/api'
export BAM_NAME='BarclaysStreamingPlatform'

python scripts/main_local.py --config utility/connector_config_local_bsp.yaml --limit 4
```

Two config files support it: [`utility/connector_config_local_bsp.yaml`](utility/connector_config_local_bsp.yaml)
(SECURE registry, no `csm:` section) and [`utility/bsp_config_local.yaml`](utility/bsp_config_local.yaml)
(the BSP client's own librdkafka YAML). Both name *environment variables* rather than values, so
neither holds a credential.

Three properties of this path are deliberate, and worth knowing before changing it:

- **The Kafka key is the trigger ID**, as the Trigger Backbone expects, and no headers are sent.
- **Records are serialised before `produce()`**, not by a `SerializingProducer`, so the size guard
  sees the exact on-the-wire bytes.
- **A blocking schema drift aborts the run**, because records are written with the *local* schema
  under the *registered* schema's id — publishing through a mismatch produces messages the
  consumer silently mis-reads.

### Local broker

`utility/connector_config_local.yaml` omits `bsp_config_path` and connects straight to
`localhost:9092`. There is no BAM authentication on that path, which is why the config validator
refuses it with `schema_registry.mode: SECURE`. DEV does not look the schema id up, so it must be
pinned with `schema_registry.schema_id` (or `IFC_SCHEMA_REGISTRY__SCHEMA_ID`); the connector
refuses to start without one. Records are still written in Confluent wire format, because the
consumer's `KafkaAvroDeserializer` rejects anything else. DEV skips the drift check, so the
pinned id must belong to a schema that matches the bundled `.avsc`.

```bash
python scripts/main_local.py --config utility/connector_config_local.yaml
```

### ECS entry point against a local data folder — no S3

The Trigger Event BDP source is not built yet, so `utility/connector_config_ecs_local.yaml` runs
`main_ecs.py` against [`data/`](data) instead of an S3 prefix. Everything downstream — envelope,
payload contract, Avro, Schema Registry, topic — is the UAT path unchanged; only the source differs.

```bash
APP_CONFIG_PATH=utility/connector_config_ecs_local.yaml IFC_RUN__TRIGGER=TRIGGER_8 python scripts/main_ecs.py
```

Four things are switched off, all in config. **No code is commented out**: paths are read as S3
when they start with `s3://` and as local files otherwise, and `AuditWriter` already degrades to
log-only without a bucket.

| Setting | Local value | Effect |
|---|---|---|
| `source.type` / `trigger_paths` | `local` / `data/trigger_<n>` | Each trigger reads its own folder, not the BDP prefix |
| `source.archive_path` | `null` | Nothing to archive — a local file is not the regulatory record |
| `audit.bucket` | `null` | Manifest to the log; no S3 writes |
| `state.backend` | `memory` | No DynamoDB. Re-running the same data republishes it |
| `run_marker.path` | `null` | The invocation gate is off; the run proceeds on any day |

Each of those has its UAT value on the line below it, commented, so restoring the S3 source is
uncommenting four blocks.

`kafka.bsp_config_path` points at the **bundled** BSP client YAML,
[`utility/bsp_config_local.yaml`](utility/bsp_config_local.yaml), for the same reason — the UAT
value reads it over `s3://`. A relative path resolves against the connector root, so nothing
needs copying. That file uses PLAINTEXT on 9092; confirm the UAT cluster accepts it from where you
run.

A run with no broker reachable still proves the source end: expect `records_parsed: 3`,
`published: 3`, `acked: 0` in the manifest, and `RECONCILIATION_FAILED` because nothing was
acknowledged. With the broker up, `acked` matches `published` and the outcome is `SUCCESS`.

### Container

[`Docker/Dockerfile`](Docker/Dockerfile) builds on the Barclays RHEL 8 Python 3.12 builder:

```
container-base.container-registry-prod.barcapint.com/barclays/devtooling/builders/rhel8/python312
```

That base already provides `python` 3.12 on PATH, so there is **no interpreter install and no
`apt-get`** — only `ca-certificates` and `curl` come from `yum`. Two stages: wheels are built where
the toolchain exists, the runtime image carries none.

The build **context is the connector directory**, not `Docker/`. It has to be: every `COPY`
reaches into `utility/`, `scripts/`, `samples/` and `data/`, and Docker refuses to copy from
outside the context. Pass the file with `-f` and the context as the final argument.

```bash
docker build -f Docker/Dockerfile -t ifc-trigger-connector:0.0.4 .
```

> **The app directory must be named `ifc_trigger_connector`.** Every module imports
> `ifc_trigger_connector.utility.*`, so with `/app` on `PYTHONPATH` the package directory has to
> carry exactly that name. Copying to `/app/platform-ifc-infrastructure` builds and pushes
> perfectly but dies on the first `docker run` with
> `ModuleNotFoundError: No module named 'ifc_trigger_connector'` — and a hyphenated name can never
> be a Python package regardless. The image is still labelled and tagged for the
> `platform-ifc-infrastructure` application; the directory name is an *import path*, not the
> deployment name. To make the two match, the package itself has to be renamed
> (`platform_ifc_infrastructure`, underscores) across every import.

`data/` must be copied too while the Trigger Event BDP source is unbuilt —
`connector_config_ecs_local.yaml` reads `data` as its source, and without it a container run
reports `ZERO_RECORDS`.

`tzdata` is in `requirements.txt` deliberately: [`utility/run_gate.py`](utility/run_gate.py)
resolves the weekday in `Europe/London` through `zoneinfo`, and a slim RHEL image may carry no
system zone database. Without it the gate silently falls back to UTC, which misreads a run
started late on a Sunday evening in BST.

**Barclays root CA.** On RHEL the supported route is the anchors directory plus a re-extract,
rather than overwriting the extracted bundle (which the next `update-ca-trust` run would undo):

```dockerfile
COPY certs/barclays-root-ca.pem /etc/pki/ca-trust/source/anchors/
RUN update-ca-trust extract
```

That produces `/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem`, the path
`utility/csm_aws_fetch.py` and the CSM config already expect.

`.dockerignore` stays at the context root, which is where both the classic builder and BuildKit
look for it.

`bsp_python_client` is proprietary to Barclays and served from the internal package index, not
PyPI. It is an ordinary line in `utility/requirements.txt`, so the build installs it alongside
everything else — there is no vendored wheel. A container does not inherit the host's pip
configuration, so the image must be able to resolve that index from inside the build (a
`pip.conf` layer, or `PIP_INDEX_URL` in the build environment).

---

## Configuration

Three layers, lowest precedence first: model defaults → the YAML at `--config` /
`APP_CONFIG_PATH` (local or `s3://`) → `IFC_` environment variables, where a double underscore
separates sections:

```
IFC_KAFKA__TOPIC=tc01_fncmtrgrbb_ifc_tbb_kyc_refresh
IFC_RUN__MODE=service
IFC_SOURCE__PATH=s3://bucket/prefix/
```

That last layer is what lets one published config serve every task, with per-task overrides in
the task definition and no redeployment of the config object.

No secret is ever configuration. CSM supplies the BSP system-account credential at runtime using
the ECS task role; only the secret's *path* is configured.

Supplied configs: [`utility/connector_config.yaml`](utility/connector_config.yaml) (UAT; values
needing confirmation are marked `CONFIRM`) and
[`utility/connector_config_local.yaml`](utility/connector_config_local.yaml).

---

## The upstream reconciliation gate

Before any work, the connector asks whether upstream produced anything to run for. TED writes one
recon document per trigger per month:

```
s3://<recon-bucket>/.../ifc-bdp-audit-recon-json/trigger8/SEPTEMBER_2026/
    BDP_Corp_Trigger_8_recon_20260930_143022_123456.json
```

Same rules as the source extracts: the folder name carries `{MONTH}_{YYYY}` expanded from the run
date, and the newest document in it wins, ordered by the timestamp *parsed* from the filename
rather than by the name. Each document holds a single record:

```json
{"target_table_name": "bdp_corp_ifc_trigger_8", "last_modified_ts": "2026-09-30T14:30:22.123",
 "status": "SUCCESS", "source_count": 412, "target_count": 412, "error_record_count": 0}
```

`status` is strictly `SUCCESS` or `RECON_FAILED`. An upstream job that failed outright writes no
document at all, so an empty folder is its own signal.

| Condition | Outcome | Exit | Meaning |
|---|---|---|---|
| folder or document absent | `UPSTREAM_DATA_NOT_RECEIVED` | 22 | upstream data never arrived, including an outright job failure |
| `last_modified_ts` not in the execution month | `UPSTREAM_PROCESSING_NOT_DONE` | 22 | upstream has not processed this month |
| `status` is `RECON_FAILED` | `UPSTREAM_JOB_FAILED` | 21 | the upstream job failed |
| `SUCCESS` but `source_count != target_count` | `UPSTREAM_COUNT_MISMATCH` | 21 | upstream should have said `RECON_FAILED` and did not |
| `SUCCESS`, both counts zero | `NO_DATA_THIS_MONTH` | 0 | a genuine month with no data — **a success**, see below |
| unreadable, unparseable, missing fields, or a status outside the contract | `UPSTREAM_RECON_UNREADABLE` | 20 / 14 | the document cannot be trusted |
| `SUCCESS`, counts equal and non-zero | proceeds | — | |

Exit codes are shared — two use 22 (`TED_MISSING_SOURCE_DATA`), two use 21 (`TED_JOB_FAILURE`) —
because those are the catalogue scenarios they belong to. The **outcome name** is what tells them
apart in the stopped-task record and the run summary, so the names are deliberately kept
distinct. A block is a reported failure, not a silent skip: non-zero exit, plus the same alert a
run failure raises, because nobody reads the logs on a monthly schedule.

**A genuine empty month is not a failure.** When upstream reconciles `SUCCESS` with zero source and
target records there is nothing to publish, and that *is* the month delivered. The run stops at
the recon document as usual, but it exits `0`, raises no RTB alert, records `SUCCESS` with
`records_processed = 0` in the run marker file (so the rest of the window stands down), and sends
the [batch completion notification](#the-batch-completion-notification) with
`No_Of_Messages_Produced = 0`.

**Stopping is the safe direction.** Because the contract is exactly two statuses, a third value is
treated as an untrusted document rather than a third outcome to interpret — there is no state in
which an unknown status is a green light. Counts that disagree are upstream's own definition of a
failed reconciliation, so a `SUCCESS` carrying them is a contradiction and the run stops; that is
the backstop for upstream having missed it. A non-zero `error_record_count` alongside matching
counts is logged but does not block.

**Nothing downstream is touched when the gate blocks.** A zero-count month, a count mismatch and a
failed upstream job all stop at the recon document: no preflight, no Kafka, no BSP, and the source
extract is never read.

**Ordering.** It runs *after* the invocation gate's weekend and already-delivered skips, and
before preflight, Kafka, the source and the marker claim. Those skips are days the run is not
meant to happen at all; checking upstream on them would alert six times a month for nothing.

`recon.enabled: false` switches it off, and so does leaving the location unset — the same
convention as `run_marker.path`, so a local run needs no recon feed. The task role needs
`s3:GetObject` and `s3:ListBucket` on the recon prefix (`ReadUpstreamReconciliation` in
`deploy/iam-task-role-policy.json`). All three per-trigger prefixes are confirmed with the data
team.

---

## The invocation gate

EventBridge cannot tell a weekday from a weekend, so it fires on **three consecutive dates per
trigger** and the container decides. Three consecutive dates always contain at least one weekday.

| Trigger | Dates fired | Published as |
|---|---|---|
| `TRIGGER_8` | 3rd, 4th, 5th | `NewHRCRelationship` |
| `TRIGGER_9` | 8th, 9th, 10th | `AccountInactivity` |
| `TRIGGER_21` | 15th, 16th, 17th | `MultipleTMSARs` |

Nine invocations a month; six are expected to do nothing. The scheduler names the trigger, either as
a command argument (`main_ecs.py trigger 9`, `"trigger 9"` or `--trigger TRIGGER_9`) or as
`IFC_RUN__TRIGGER` (e.g. `TRIGGER_9`) in the EventBridge Scheduler's container override — the
variable agreed with DevOps; the argument wins. Any common spelling (`trigger 9`,
`TRIGGER-9`, `9`) is normalised to `TRIGGER_9`, so the run marker is the same whichever is used.
There is no default, because defaulting would silently publish the wrong trigger's month.

The trigger also chooses **where the data is read from**: `source.trigger_paths` maps each trigger to
its own S3 location. Precedence is an explicit `data_path` event value, then the trigger's
`trigger_paths` entry, then `source.path`. The UAT config sets no `source.path`, so a trigger without
an entry fails at startup instead of reading another trigger's data.

[`utility/run_gate.py`](utility/run_gate.py) runs **before** preflight, Kafka or the source, so a
weekend start costs one container start and nothing else:

1. **Weekend** → exit `0`. Public holidays are deliberately not modelled: the business confirmed a
   bank holiday Monday is an acceptable delivery day.
2. **Month already delivered** → exit `0`.

Both skips exit `0`. Six no-op invocations a month *is* the scheme working; a non-zero exit would
alarm on all of them.

The marker is a line in the **run marker file** — a JSON Lines file in S3 (`run_marker.path`),
one line appended per invocation. DynamoDB is not an approved service, so there is no table: the
file is both what the gate reads and what Athena queries. A month reaches **`SUCCESS` only when a
run actually delivered records**. A run that found nothing records `NOT RAN` or `FAILURE`, so the
next date in the window retries; that is the Databricks-failure case, and it is also why there is
nothing to roll back when a run fails. Marking a month `SUCCESS` on an empty run would burn the
remaining invocations *and* block the manual re-run.

The marker's month is the **run** month: a run on 3 September records `year_month = 2026-09`. The
records that run publishes belong to the business month before it — August — and carry
`2026-08-31T23:59:59.999999999Z` as their `timestamp`.

Each line of `run-markers/run_markers.json`:

```json
{"trigger_id":"TRIGGER_8","year_month":"2026-09","run_date":"2026-09-03","run_status":"SUCCESS","records_processed":150,"reason":"","recorded_at":"2026-09-03T02:04:11.512Z"}
{"trigger_id":"TRIGGER_8","year_month":"2026-09","run_date":"2026-09-04","run_status":"NOT RAN","records_processed":0,"reason":"TRIGGER_8 2026-09 was already delivered","recorded_at":"2026-09-04T02:03:52.107Z"}
```

| Field | Type | Example |
|---|---|---|
| `trigger_id` | string | `TRIGGER_8` |
| `year_month` | string | `2026-09` |
| `run_date` | string | `2026-09-03` — the date the gate looked |
| `run_status` | string | `SUCCESS` \| `FAILURE` \| `NOT RAN` |
| `records_processed` | number | `150` — delivered; `0` for anything but `SUCCESS` |
| `reason` | string | why — `2026-10-03 is a Saturday`, `TRIGGER_8 2026-09 was already delivered`, `UPSTREAM_DATA_NOT_RECEIVED`, `exit_code=…` |
| `recorded_at` | string | `2026-09-03T02:04:11.512Z` — when the line was written (UTC) |

**Why one object per line, not a bracketed `[ … ]` array.** Athena's JSON SerDe reads one JSON
object per line as one row; a pretty-printed or bracketed array is read as a single unparseable
row. JSON Lines is still an array in every sense that matters — an ordered list of records,
appended to — and any tool reads it with `[json.loads(l) for l in f if l.strip()]`.

**Every outcome is recorded, not only delivery**, so the file answers "what happened to this
trigger's September" rather than only "was it delivered":

| What happened | `run_status` |
|---|---|
| the month was delivered | `SUCCESS` |
| the connector ran and did not deliver | `FAILURE` |
| a weekend, upstream had nothing ready (any recon-gate block), or the month was already delivered | `NOT RAN` |

The file is a history, and **a month's outcome is its latest `SUCCESS` or `FAILURE` line**.
`NOT RAN` lines never change it — they record that a date looked and did nothing. `SUCCESS` is
the only outcome that stops a later invocation; a `FAILURE`, or only `NOT RAN` lines, means the
month still has to be retried.

**Delivered on the 3rd, triggered again on the 4th:** the 4th reads the `SUCCESS`, does not run,
and appends `NOT RAN` with reason `TRIGGER_8 2026-09 was already delivered`. The 3rd's `SUCCESS`
line is untouched and still decides the month, so the 5th stands down the same way. The run
summary and audit report those invocations as `SKIPPED`. The delivered check runs ahead of the
weekend check, so the Saturday after a delivered Friday records "already delivered" rather than
"is a Saturday". `IFC_RUN__FORCE=true` overrides it, being a deliberate re-delivery; if a forced
run then fails, its `FAILURE` is the latest outcome and reopens the month.

**Appending to S3.** S3 has no append, so each write reads the file, adds one line and puts it
back with a conditional PUT — `If-Match` on the ETag it read, or `If-None-Match: *` when creating
the file. All three triggers share one file, so two finishing together would otherwise drop one
line; the loser gets `412 PreconditionFailed`, re-reads and retries (up to five times). S3 reads
are strongly consistent, so a delivered month is never read as open. This needs **boto3 /
botocore 1.35.69 or later** (the first to accept `IfMatch`), pinned in `requirements.txt`. A line
that is not valid JSON fails the gate rather than being skipped — the skipped line might be the
`SUCCESS`. The task role needs `s3:GetObject` and `s3:PutObject` on the `run-markers/` prefix and
`s3:ListBucket` on it, without which a not-yet-created file reads as `AccessDenied` rather than
"missing" (`RunMarkerFile` / `RunMarkerFileExistence` in `deploy/iam-task-role-policy.json`).
Keep bucket versioning on: every append rewrites the object, and the file is delivery evidence.

**Athena.** [`deploy/athena-run-markers.sql`](deploy/athena-run-markers.sql) creates
`ifc_trigger_connector_run_markers` over the `run-markers/` folder (keep nothing else in it —
Athena reads every object under `LOCATION`) and a `ifc_trigger_connector_run_marker_latest` view
that applies the same rule as the gate — one row per trigger and month, with `last_checked` and
`invocations` showing the `NOT RAN` checks after it:

```sql
SELECT trigger_id, run_status, run_date, records_processed
FROM ifc_trigger_connector_run_marker_latest
WHERE year_month = '2026-09';
```

Writing the marker is best-effort: the run has already happened, so a failed write is logged
loudly rather than failing the task — but a lost `SUCCESS` means the next invocation republishes
the month, so the log line says exactly that.

Set `IFC_RUN__FORCE=true` to re-deliver a month after its window has passed; it skips both checks.
Gating applies only to a batch run with `run_marker.path` set, so local runs need neither a
trigger nor a marker file. `run_marker.path` also accepts a local file path, which switches the
gate on for a local run without S3. A config that still sets the old `run_marker.table_name` is
rejected at startup rather than ignored — ignoring it would switch the gate off silently.

---

## Exit codes

The exit code identifies the scenario, so an ECS stopped-task record routes the incident without
log archaeology. Grouped: **10–19** infrastructure, **20–29** data/contract, **30–39** platform,
**40–49** security.

| Code | Scenario | Code | Scenario |
|---|---|---|---|
| 0 | Success | 23 | Reconciliation failure |
| 75 | Drained on SIGTERM, work remaining (benign) | 24 | Message too large |
| 10 | Container failure | 30 | High publish latency |
| 11 | Out of memory | 31 | Partition leader failure |
| 12 | Network connectivity | 32 | Broker unavailable |
| 13 / 14 | Trigger BDP write / read | 33 | Topic unavailable |
| 15 / 16 | FRED processing / audit store | 34 | Schema Registry unavailable |
| 20 | Schema validation | 40 | Authentication failure |
| 21 / 22 | TED job failure / missing source data | 41 | Authorisation failure |

---

## Deployment

Artefacts in [`deploy/`](deploy):

| File | Contents |
|---|---|
| `ecs-task-definition.json` | Fargate task definition. `stopTimeout: 120` **must** exceed `run.shutdown_grace_seconds` (90), or SIGKILL wins and the drain is lost |
| `iam-task-role-policy.json` | Least-privilege task role, including an explicit **Deny** on deleting from the Trigger BDP — the connector archives by copy, never deletes |
| `infrastructure.json` | The run-marker file location, the three EventBridge trigger schedules *or* the continuous ECS service, egress security group (9095/8095/BAM/CSM), log retention |
| `athena-run-markers.sql` | Athena table and latest-state view over the run marker file |
| `cloudwatch-alarms.json` | One alarm per observable scenario, each naming the catalogue scenario it detects |

Two deployment modes, same image and task definition:

- **`run.mode: batch`** with EventBridge Scheduler — one schedule per trigger, each firing on
  three consecutive dates (`cron(0 3 3,4,5 * ? *)` and so on). The container decides whether to
  process; see [The invocation gate](#the-invocation-gate). The task drains and exits.
- **`run.mode: service`** as a resident ECS service polling the prefix. Keep `desiredCount: 1`
  unless the DynamoDB state backend is in use; the S3 backend cannot allocate sequence numbers
  safely across concurrent tasks.

Health endpoints on `:8080` — `/health/live` (restart me), `/health/ready` (stop routing to me),
`/health/startup` (preflight done), `/metrics`. A task that has lost its BSP connection but is
still running reports **not ready**, which is the most dangerous form of container failure:
nothing crashes, and nothing publishes.

---

## The batch completion notification

When a batch finishes cleanly — or upstream reconciles a genuine empty month — the ECS entry point
publishes one SNS event to `notifications.batch_sns_topic_arn`. It is **not** an alert: the Trigger Backbone starts its
downstream processing from it, so it is a business event on its own topic, separate from the RTB
failure alert on `notifications.sns_topic_arn`.

[`utility/trigger_batch_notifier.py`](utility/trigger_batch_notifier.py) holds the message;
`_publish_batch_notification` in [`scripts/main_ecs.py`](scripts/main_ecs.py) decides whether to
send it.

| Field | Value |
|---|---|
| `Trigger_Originating_BU` | `notifications.trigger_originating_bu` (default `UK-C`) |
| `No_Of_Messages_Produced` | acknowledged messages — what the broker confirmed, not what was attempted |
| `Trigger_Sub_Type` | the published sub-type the batch carried |
| `Topic_Name` | `kafka.topic` |
| `Trigger_Batch_Start_Timestamp` / `..._End_Timestamp` | earliest and latest `triggerPostingTimestamp` in the batch |
| `Event_Timestamp` | when the notification itself was raised (UTC, milliseconds) |
| `Correlation_Id` | the run id, so the event joins the run manifest and the logs |

**A genuine empty month** (`NO_DATA_THIS_MONTH`) sends the same body, with no batch to draw on:

| Field | Empty-month value |
|---|---|
| `No_Of_Messages_Produced` | `0` |
| `Trigger_Sub_Type` | the published sub-type of the trigger this invocation ran for |
| `Trigger_Batch_Start_Timestamp` / `..._End_Timestamp` | both the current time, in the same RFC 3339 form as `triggerPostingTimestamp` |
| `Correlation_Id` | the run id of the `RECON_GATE` manifest the invocation wrote |

It is sent once per month: the `SUCCESS` it records closes the month, so later dates in the window
stand down. The enable switch and topic setting below apply to it the same way.

`trigger_sub_type`, `topic_name` and `correlation_id` also go out as SNS message attributes, so a
subscriber can filter without parsing the body.

**When it is withheld.** Announcing an incomplete batch would start downstream work on a topic
that is missing records, so for a publishing run the event is sent only when *all* of these hold:
the run outcome is `SUCCESS`, reconciliation balanced, at least one message acknowledged, and no
delivery failures or unflushed messages. A run whose source turned out empty without upstream's
recon confirming zero is still withheld — only the recon document can say a month is genuinely
empty. A run that never produced a batch (a start-up failure) sends nothing.

**When it is disabled.** `batch_notifications_enabled: false` turns it off; so does leaving
`batch_sns_topic_arn` null, which is the local DEV default — the notifier then logs the payload it
would have sent and makes no AWS call.

Publishing is best-effort at the entry point: a broken SNS topic is logged, and does not turn a
successful batch into a failed ECS task.

**IAM.** The task role needs `sns:Publish` on this topic as well as on the alerting topic —
`deploy/iam-task-role-policy.json` carries both, under `RtbAlerting` and
`TbbBatchCompletionEvent`. A region must also be resolvable (`AWS_REGION` in the task
definition): `boto3.client("sns")` takes no explicit region, the same as the failure notifier.

---

## Contract drift found in the source documents

Points where the source material is internally inconsistent. Each is handled as stated; all need
confirming with TBB.

1. **The payload JSON Schema has a misplaced key.** `additionalProperties` sits *inside*
   `properties`, which declares a property literally named `additionalProperties` rather than
   restricting the object. Corrected in the bundled copy.
2. **Timestamp format is unspecified.** The document pins no format. This connector emits
   ISO-8601 with milliseconds (`2026-06-10T02:15:04.221Z`). Other producers on the topic have
   used `%Y%m%dT%H%M%S%fz`; the two are not interchangeable and a consumer will parse one of
   them, so the format needs agreeing rather than assuming.
3. **The Main schema's field table omits `triggerOriginatingBU`**, which the Avro definition
   requires. Treated as mandatory, per the Avro.
4. **Namespaces are inconsistent** across the four schemas — the Main schema uses
   `…consumer.bsp.schema.model` while ACK, DLQ and Outcome use `…producer.…`. Transcribed as
   published; harmless for Avro resolution, which matches by field, and logged as INFO.

---

## Open items before UAT

Environment readiness, mirroring the POC assessment's own list. None is an application design
problem.

| Item | Status |
|---|---|
| Firewall rules implemented for AWS source → BSP destination CIDRs | Described, not confirmed implemented. Preflight will prove it in seconds |
| DNS resolution and routing from BB BCA subnets to intranet BSP hosts | Needs confirmation |
| Actual system account name and CSM secret path | Placeholder in `utility/connector_config.yaml` |
| Confirmed IFC CDD topic name and registry subject | `tc01_fncmtrgrbb_ifc_tbb_kyc_refresh` assumed from the topic table |
| Kafka ACLs for the producer principal on the topic | Needed; preflight distinguishes a missing ACL from a missing topic |
| **Tokenisation policy names** for account fields | Only `DPASS_POLICY_NAME` (Client Relationship Owner Name) is confirmed on Confluence; `POLICY_ACCOUNT` in `utility/trigger_payload.py` is a placeholder |
| Timestamp format agreement with TBB | See drift item 4 |
| Trigger 9 and 21 data sources | Both marked "under investigation" in the business data product; the payload definitions are complete, the source keys may need remapping |
| Internal package index reachable from the image build, for `bsp_python_client` | Required; it is an ordinary pip requirement |
| SNS topic for RTB alerting | `notifications.sns_topic_arn` is null; alerts currently log only |
| SNS topic for the TBB batch-completion event | `notifications.batch_sns_topic_arn` is null; the event currently logs only. Confirm the topic and the `Trigger_Originating_BU` value with TBB |
