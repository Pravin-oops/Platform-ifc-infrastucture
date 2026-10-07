# IFC Trigger Connector

A custom Kafka connector that runs on **AWS ECS** and publishes **IFC CDD trigger events
(Triggers 8, 9 and 21)** from the Trigger Event BDP onto the **BSP on-prem Trigger Backbone**.

It runs once a month per trigger, reading that month's rows straight from the trigger's
**Athena (Iceberg) table**, and every design decision below follows from that: a container
publishing a month in one run, on a schedule where nobody is watching the logs, has to leave
evidence rather than assume someone saw it fail. Athena is the only permitted source; there is
no S3 file or JSON extract path.

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

**In scope.** Reading detected trigger events from the Trigger BDP tables through Athena; building the
`TriggerBackboneTopicSchema` envelope and the BSP-format payload for Triggers 8, 9 and 21;
authenticating to BSP through CyberArk and BAM; validating against the BSP Schema Registry;
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
| **The batch is one month of rows** | Query results are streamed page by page and the producer queue is capped; `BufferError` applies back-pressure instead of growing the heap | Loading a month's records eagerly is how a container gets OOM-killed (exit 137) |
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
              Trigger Event BDP (Iceberg tables, one per trigger)
                                          │
                                          ▼
                         FRED (ECS) ── resolves event actions
                                          │
                                          ▼
        ┌──────────────────── THIS CONNECTOR (ECS) ────────────────────┐
        │  recon gate ─► run gate ─► preflight ─► Athena query ─►      │
        │  build envelope ─► validate ─► serialise ─► size guard ─►    │
        │  publish ─► flush ─► reconcile ─► notify                     │
        │        │                        │                            │
        │        ▼                        ▼                            │
        │  S3 run-marker JSON       S3 quarantine + manifest           │
        └──────────────┬─────────────────────────┬─────────────────────┘
                       │ CyberArk → BAM → JWT    │
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
Platform-ifc-infrastructure/      # the app root; imports are utility.*
|
├── .env                          # developer env values, loaded by the entry points
├── Docker/Dockerfile             # single-stage image; ENTRYPOINT is scripts/main_ecs.py
|
├── scripts/                      # Executable entry points
│   ├── main.py                   # CLI: run, catalogue
│   └── main_ecs.py               # ECS platform entry: APP_CONFIG_PATH -> exit code
|
├── utility/
│   ├── __init__.py
│   ├── connector_utility.py      # S3/local path handling, loaders, resource resolution
│   ├── connector_config.py       # typed config; YAML + IFC_ env overlay
│   ├── connector_runner.py       # the batch run: query, publish, reconcile
│   ├── athena_query.py           # run one Athena query, read typed rows back
│   ├── trigger_source.py         # Athena trigger-table reader
│   ├── recon_gate.py             # upstream reconciliation gate (Athena recon table)
│   ├── run_gate.py               # weekend / already-delivered gate, run markers
│   ├── sequence_allocator.py     # batch sequence numbers
│   ├── audit_utility.py          # quarantine, run manifest, reconciliation
│   ├── health_utility.py         # /health/live, /ready, /startup, /metrics
│   ├── resilience_utility.py     # backoff, circuit breaker, SIGTERM drain
│   ├── observability_utility.py  # JSON logs, CloudWatch EMF metrics
│   ├── cyberark_ccp_fetch.py     # client cert -> CyberArk CCP -> system-account credential
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
│   ├── schema.json               # TriggerBackboneTopicSchema (Avro)
│   ├── connector_config.yaml     # deployed app config (ECS, CyberArk-backed)
│   └── requirements.txt
|
└── tests/                        # pytest suite; run_tests.sh / run_tests.ps1 run it
```

The deployment artefacts referred to below as `deploy/` (task definition, IAM policy,
infrastructure, alarms, Athena DDL) are not kept in this repository.

The import root is the repository root: modules import `utility.*`. Each entry point puts its
own app root on `sys.path` and loads `<app root>/.env` (without overriding variables already
set), so `python scripts/main_ecs.py` works from any directory.

---

## Failure scenario coverage

Every row of the agreed *Failure scenarios to be worked on* matrix is encoded in
[`utility/failure_catalog.py`](utility/failure_catalog.py) with the owning team, the agreed
action, an exit code, and what the connector does automatically.

`python scripts/main.py catalogue` prints the whole thing as JSON.

### Handled at the connector

| Scenario | Handling | What the connector does |
|---|---|---|
| **Producer Container Failure** | graceful drain | SIGTERM stops intake, flushes in-flight messages within the grace window, exits 75; the run marker records FAILURE so the next date in the window re-runs the month |
| **Producer Out Of Memory** | back-pressure | Streaming reads (never a full listing in memory), capped producer queue, `BufferError` waits instead of growing the heap; RSS and RSS/limit published so the alarm precedes exit 137 |
| **Network Connectivity Failure** | preflight abort | DNS resolution and a TCP connect to every broker (9095) and the registry (8095) before authenticating — the exact evidence a firewall request needs |
| **Trigger BDP Read Failure** | preflight abort | Source prefix listed before any BSP handshake, so a permission or path error is reported in seconds |
| **Schema Validation Failure** | quarantine | Three gates — payload vs BSP JSON Schema, envelope vs local `.avsc`, local `.avsc` vs registered subject. Bad records go to S3 quarantine; the batch continues; registry drift aborts at startup |
| **FRED Audit Store Failure** | reconcile + report | The S3 quarantine and the run manifest are the audit record; a rejected record is recoverable from the quarantine object alone |
| **Zero records (TED job failure / missing source data)** | reconcile + report | An empty source on a scheduled run exits non-zero as `ZERO_RECORDS`, so a silent upstream failure cannot look like a clean run |
| **Producer Reconciliation Failure** | reconcile + report | Manifest with full counts, per-partition offset ranges, and a balance check that fails the run when it does not hold |
| **High Kafka Publish Latency** | retry + backoff | Ack-latency and queue-depth metrics; a failed publish is retried with jittered backoff up to `resilience.max_publish_attempts`, every failed attempt counting towards the breaker; a record that still fails is not quarantined, so the reconciliation fails the run |
| **Message Too Large** | quarantine | Serialised size measured against the 800 KB limit *before* `produce()`; oversized records quarantined with a payload/overhead breakdown |
| **Kafka Partition Leader Failure** | retry + backoff | Idempotent producer + librdkafka metadata refresh; escalates only past the breaker threshold |
| **Broker Unavailable** | retry + backoff | Jittered backoff; after N consecutive failures the run is abandoned cleanly and recorded as FAILURE |
| **Topic Unavailable / Incorrect Topic** | preflight abort | Cluster metadata for the topic, with a partition-count assertion — a typo fails in seconds |
| **Schema Registry Unavailable** | retry + backoff | Schema id resolved once and cached, so a mid-run outage does not stop publishing |
| **Authentication Failure** | preflight abort | CyberArk CCP, BAM and a registry call all happen before any record is read; JWT shape checked and `exp` tracked with pre-emptive refresh |
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

Trigger 21 publishes the same header columns as Triggers 8 and 9, and its table has the same
columns: nothing in its definition is a guess at an alert-volume column.

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
  carries `UK`. If the table ever holds a city name, those rows are quarantined by design;
  the fix is upstream, not a wider column here.
- **Relationship Owner Name is fifty characters and is tokenised.** The limit is measured on the
  tokenised value the connector receives, not on the cleartext name. If DPASS tokens run longer
  than the cleartext they replace, this width needs re-confirming.

### Input contract

**Each trigger has its own Athena (Iceberg) table**, built by the dbt models on Databricks and
named in `source.trigger_tables` as `database.table`. `IFC_RUN__TRIGGER` picks the table, so a
run only ever reads its own trigger's data; an unmapped trigger fails at startup.

The SIT tables are `bdb_ifc_synthetic_data_test.bdp_corp_ifc_trigger_8`, `_9` and `_21`. All
three have the same columns:

| Column | Type | Used for |
|---|---|---|
| `date_of_request` | `string` | Date of Request (date part published); first sort key |
| `counterparty_full_legal_entity_name` | `string` | Counterparty Full Legal Entity Name |
| `counterparty_csid_sds` | `bigint` | Counterparty ID and the envelope's `idValue`; second sort key |
| `customer_segment` | `string` | not published |
| `client_relationship_owner_brid` | `string` | Client Relationship Owner BRID |
| `client_relationship_owner_name` | `string` | Client Relationship Owner Name (tokenised) |
| `client_relationship_owner_business_unit` | `string` | Client Relationship Owner Business Unit |
| `client_relationship_owner_location` | `string` | Client Relationship Owner Location |
| `region` | `string` | Region |
| `business_date` | `date` | the month filter, and the sub-event discriminator; not published |

Query results go to `s3://sit1-logs-corpdeng-509153454187-eu-west-1/athena_output/`
(`source.athena.output_location`, the DevOps stack's results bucket and prefix).

**The tables hold only the attribute columns.** Nothing on a row says which trigger it belongs
to: the run's trigger does. Each row's columns become the event's `attributes`, and the envelope
takes its identity from the trigger:

| `IFC_RUN__TRIGGER` | `triggerType` | `triggerSubType` |
|---|---|---|
| `TRIGGER_8` | `KYCRefresh` | `NewHRCRelationship` |
| `TRIGGER_9` | `KYCRefresh` | `AccountInactivity` |
| `TRIGGER_21` | `KYCRefresh` | `MultipleTMSARs` |

**The month is selected by `business_date`.** Upstream stamps every row of a month with that
month's last day, so a run reads the rows equal to the last day of the previous month in
`Europe/London` — any run in September 2026 executes:

```sql
SELECT * FROM "<database>"."<trigger table>"
WHERE CAST("business_date" AS DATE) = CAST(? AS DATE)   -- '2026-08-31'
ORDER BY "date_of_request", "counterparty_csid_sds"
```

Table and column names are validated as plain identifiers and quoted; the date is bound as a
query parameter. `order_by` fixes the row order so a re-run allocates the same sequence numbers.
Results are paged through `GetQueryResults` (1000 rows a page) and each column's Athena type is
restored — a `bigint` CSID arrives as an int, a `date` as a date.

**Only Athena.** `source` accepts `trigger_tables`, `table` and the `athena` block, and rejects
anything else — a config still carrying the old S3 extract settings
(`type`, `path`, `trigger_paths`, `file_suffixes`, `archive_path`, ...) fails at load rather than
being silently ignored.

A query that **cannot be run** — a missing table, `AccessDenied`, a failed, cancelled or
timed-out query — fails the run as `SOURCE_UNREADABLE` with the `TRIGGER_BDP_READ_FAILURE` exit
code — the reader raises `SourceAccessError`, and nothing is published. A query that runs and
finds **no rows** is different and stays `ZERO_RECORDS`.

Preflight checks the table with `GetTableMetadata` — a Glue catalogue lookup that scans no data
but exercises the same permissions the query needs.

**Reprocessing a past month.** Set `IFC_RUN__MONTH` (`run.month`) on a one-off RunTask to run as
that month instead of the current one. It takes `2026-08` or `AUGUST_2026`. An October run with
`IFC_RUN__MONTH=2026-08` behaves as the August run did: it queries `business_date = 2026-07-31`,
stamps records `2026-07-31T23:59:59.999999999Z`, and records its outcome against `2026-08` in the
run marker. Three things stay on the real date: the weekend check, the marker line's `run_date`,
and the **recon check** — upstream writes its recon row when it runs, so that October run needs a
recon row whose `last_modified_ts` is in October. A month already marked `SUCCESS` is still skipped
unless `IFC_RUN__FORCE=true` is set too, so the override alone cannot republish a delivered month
by accident. Leave it unset on the schedule.

Upstream supplies no customer id, business unit or timestamp — the connector derives them:

| Envelope field | Value |
|---|---|
| `triggerID` (and the Kafka key) | `{system}_{triggerType}_{triggerSubType}_{timestamp}_{sequenceNumber}` — see below |
| `triggerType` | fixed: `KYCRefresh` for every trigger |
| `triggerSubType` | the published enum symbol for the trigger |
| `timestamp` | last instant of the **business month**, the month before the run month (UK time): a July 2026 run stamps every record `2026-06-30T23:59:59.999999999Z` |
| `triggerPostingTimestamp` | when the record is posted, same RFC 3339 format (UTC, nanosecond precision) |
| `sequenceNumber` | the record's position in the batch — 1, 2, 3… across all customers |
| `triggerOriginatingSystem` | the environment's service number, from `envelope.originating_systems` by `app.environment` (table below) |
| `triggerOriginatingBU` | fixed: `UK-C` |
| `idSystem` | the same service number as `triggerOriginatingSystem` |
| `idType` | fixed: `Customer` |
| `idValue` | `counterparty_csid_sds`, as a string; a row without one is quarantined |
| `upstreamTriggerID` | always null: the trigger tables carry no upstream trigger id |
| `payload` | the eight contract fields above |

The service number depends on the environment the task runs in, set by `app.environment`
(`IFC_APP__ENVIRONMENT` on the task). It is sent as both `triggerOriginatingSystem` and
`idSystem`, and starts every trigger ID. A run whose environment is not listed stops at startup
rather than publish under another environment's number.

| `app.environment` | Service number |
|---|---|
| `DEV` | `SNSVC0084379` |
| `SIT` | `SNSVC0084378` |
| `PROD-ANALYTICS` | `SNSVC0084375` |
| `PROD-PARALLEL` | `SNSVC0084371` |
| `PROD` | `SNSVC0084373` |

The sub-event discriminator is the row's `business_date`. Neither table has a sub-event column,
and the grain is one row per counterparty per business date, so without it a second row for one
counterparty inside a business month would collide with the first on trigger ID.

---

## Identity, idempotency and ordering

**Trigger ID** follows the Trigger Backbone's format:

```
{system}_{triggerType}_{triggerSubType}_{timestamp}_{sequenceNumber}
SNSVC0084378_KYCRefresh_NewHRCRelationship_2026-06-30T23:59:59.999999999Z_1
```

`{system}` is the environment's service number (`SNSVC0084378` in SIT, above).

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

The business month comes from the **run month**, not the data: it is the month before the one
the run executes in, or the one `IFC_RUN__MONTH` names. A late re-delivery of June runs with
`IFC_RUN__MONTH=2026-07` (plus `IFC_RUN__FORCE=true` if July was already delivered), which
queries the same `2026-06-30` rows and stamps them with the same month and timestamp.

**The Kafka key is the trigger ID**, as in the Trigger Backbone's reference records. It is unique
per record, so records spread across partitions and a counterparty's events are not guaranteed to
stay in order relative to each other. Records carry **no Kafka headers**.

Each built record still carries a **business key** — canonical sorted-key JSON of system, trigger
type, sub-type, business month, `idType`, the CSID, the sub-event discriminator (`business_date`
for Triggers 8 and 9) and, from a customer's second event onwards, that customer's occurrence
number. It is not published and no longer feeds the trigger ID.

Only **acknowledged** messages are counted as published. An unacknowledged message fails the
reconciliation, so the run is recorded as FAILURE and the next date in the window re-runs the month.

---

## Reconciliation and audit

`audit.bucket` takes either a bare bucket name or a full `s3://bucket/folder` URI, for evidence
that lives under a folder rather than at the root of a bucket of its own. The three prefixes are
appended to it, so a URI naming a folder nests them under that folder. Every run writes a manifest
to `<audit.bucket>/<manifest_prefix>/run_date=…/<run_id>.json`
containing the run and task identity, configuration in force, full counters, per-partition
offset ranges, the preflight report, what was read (table, business date and Athena query
execution id), quarantine object keys, and the
classified failure if there was one.

The control is a balance identity:

```
records read = published + quarantined
```

If it does not hold — or if published ≠ acknowledged, or the flush timed out with messages still
queued — the run **fails** with `PRODUCER_RECONCILIATION_FAILURE`, even when every individual
publish succeeded. An unexplained gap is exactly what the control exists to catch.

A separate quality gate fails the run when the quarantined fraction exceeds
`resilience.max_quarantine_ratio` (default 5%), so a run cannot publish a fraction of its content
and report success.

---

## Running it

### A trigger's monthly run

```bash
APP_CONFIG_PATH=utility/connector_config.yaml IFC_RUN__TRIGGER=TRIGGER_8 python scripts/main_ecs.py
```

`IFC_RUN__TRIGGER` is required: it picks the Athena table and the published sub-type. The run
needs AWS credentials that can query that table (see [Deployment](#deployment)).

### Reprocessing a past month

```bash
APP_CONFIG_PATH=utility/connector_config.yaml IFC_RUN__TRIGGER=TRIGGER_8 IFC_RUN__MONTH=2026-08 IFC_RUN__FORCE=true python scripts/main_ecs.py
```

Runs as the August run did: queries `business_date = 2026-07-31` and records the outcome against
`2026-08`. The recon check needs a recon row from the current month (October, for a run in
October). Drop `IFC_RUN__FORCE` when August was never
delivered. See [Input contract](#input-contract).

### The failure catalogue

```bash
python scripts/main.py catalogue
```

### Without BSP — DEV registry mode

A config that omits `kafka.bsp_config_path` and supplies `bootstrap.servers` in `kafka.overrides`
connects straight to a broker. There is no BAM authentication on that path, which is why the
config validator refuses it with `schema_registry.mode: SECURE`. DEV does not look the schema id
up, so it must be pinned with `schema_registry.schema_id` (or `IFC_SCHEMA_REGISTRY__SCHEMA_ID`);
the connector refuses to start without one. Records are still written in Confluent wire format,
because the consumer's `KafkaAvroDeserializer` rejects anything else. DEV skips the drift check,
so the pinned id must belong to a schema that matches the bundled `.avsc`.

### Publishing — properties worth knowing before changing it

- **The Kafka key is the trigger ID**, as the Trigger Backbone expects, and no headers are sent.
- **Records are serialised before `produce()`**, not by a `SerializingProducer`, so the size guard
  sees the exact on-the-wire bytes.
- **A blocking schema drift aborts the run**, because records are written with the *local* schema
  under the *registered* schema's id — publishing through a mismatch produces messages the
  consumer silently mis-reads.

### Container

[`Docker/Dockerfile`](Docker/Dockerfile) builds on the Barclays RHEL 8 Python 3.12 builder:

```
container-base.container-registry-prod.barcapint.com/barclays/devtooling/builders/rhel8/python312
```

That base already provides `python` 3.12 and the system CA bundle, so there is **no interpreter
install and no `yum` step**. It is a single stage: dependencies install before the code is copied,
so a code-only change reuses the cached dependency layer.

The build **context is the repository root**, not `Docker/`. It has to be: the `COPY`s reach
into `utility/` and `scripts/`, and Docker refuses to copy from outside the context. Pass the
file with `-f` and the context as the final argument.

```bash
docker build -f Docker/Dockerfile -t ifc-trigger-connector:0.0.4 .
```

`tzdata` is in `requirements.txt` deliberately: [`utility/run_gate.py`](utility/run_gate.py)
resolves the weekday in `Europe/London` through `zoneinfo`, and a slim RHEL image may carry no
system zone database. Without it the gate silently falls back to UTC, which misreads a run
started late on a Sunday evening in BST.

**Barclays root CA.** `CARoot.pem` is neither committed (`.gitignore` excludes `*.pem`, `*.crt`,
`*.cer`) nor baked into the image. It is one Secrets Manager secret holding the plain PEM,
`ca_certificate.secret_id` (`/ifc/bsp-event-processor/kafka/ca-bundle`, the template's
`KafkaCABundleSecret`, created empty and filled by hand). At start, before
anything connects, [`utility/ca_certificate.py`](utility/ca_certificate.py) reads it with the ECS
task role and writes it to `ca_certificate.path` (`/tmp/ifc-certs/CARoot.pem`). Two settings
point at that file:

- `ssl.ca.location` in [`utility/bsp_sit_config.yaml`](utility/bsp_sit_config.yaml), for the
  broker connection;
- `schema_registry.ca_location`, for the Schema Registry.

The task role needs `secretsmanager:GetSecretValue` on the secret, as it already has on the
CyberArk certificate secrets. A missing, empty or non-PEM secret fails the run before any
record is read. The CyberArk call keeps the system trust store, which already trusts the CCP
host.

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
IFC_RUN__TRIGGER=TRIGGER_9
```

That last layer is what lets one published config serve every task, with per-task overrides in
the task definition and no redeployment of the config object.

No secret is ever configuration. CyberArk CCP supplies the BSP system-account credential at
runtime, authenticated with a client certificate and key the ECS task role reads from two
Secrets Manager secrets; only the CCP query and the secrets' *names* are configured.

The CCP query comes from the ECS product template (`products/ecs/product.template.yaml`), which
sets these from its CyberArk parameters. They sit between the YAML and the `IFC_` layer, so
`IFC_CYBERARK__<FIELD>` still overrides them for a single task:

| Variable | Fills |
|---|---|
| `CYBERARK_ENABLED` | `cyberark.enabled`; `false` (the default) skips CyberArk for dev runs, which publish with whatever `BSP_USERNAME`/`BSP_PASSWORD` the environment holds |
| `CYBERARK_CCP_URL` | `cyberark.base_url`, the full `.../AIMWebService_certs/api/Accounts` URL |
| `CYBERARK_APP_ID` | `cyberark.app_id` |
| `CYBERARK_SAFE` | `cyberark.safe` |
| `CYBERARK_ACCOUNT` | `cyberark.object`, sent to CCP as `Object` |

Supplied config: [`utility/connector_config.yaml`](utility/connector_config.yaml) (SIT; values
needing confirmation are marked `CONFIRM`, including the Athena workgroup).

---

## The upstream reconciliation gate

Before any work, the connector asks whether upstream produced anything to run for. The Databricks
recon job appends **one row per model run** to a single table, `bdb_ifc_synthetic_data_test.batch_recon`,
shared by all three triggers. The connector reads it through Athena (`recon.table`, through
`source.athena`'s workgroup, catalog and result location), and each run reads only the rows whose
`target_table_name` is its own trigger's:

| `IFC_RUN__TRIGGER` | `target_table_name` in `batch_recon` (`recon.trigger_targets`) |
|---|---|
| `TRIGGER_8` | `` `sit_cds_snsvc0080860_prepared_db`.bdb_ifc_synthetic_data_test.BDP_Corp_IFC_Trigger_8 `` |
| `TRIGGER_9` | `` `sit_cds_snsvc0080860_prepared_db`.bdb_ifc_synthetic_data_test.BDP_Corp_IFC_Trigger_9 `` |
| `TRIGGER_21` | `` `sit_cds_snsvc0080860_prepared_db`.bdb_ifc_synthetic_data_test.BDP_Corp_IFC_Trigger_21 `` |

The `batch_recon` columns:

| Column | Type | Used for |
|---|---|---|
| `target_table_name` | `string` | which trigger the row is about — matched to `recon.trigger_targets` |
| `status` | `string` | `SUCCESS`, `RECON_FAILED` or `FAILED` |
| `source_count`, `target_count` | `bigint` | must match; both zero is an empty month |
| `error_record_count` | `bigint` | logged when non-zero; does not block |
| `last_modified_ts` | `timestamp` (UTC) | picks the newest row; must be in the current month |
| `model_name`, `job_run_id`, `batch_id` | `string`, `string`, `bigint` | quoted in every alert |
| `idempotency_key`, `env`, `dataproduct_name`, `last_modified_by` | `string` | carried into the run summary |

A run of Trigger 9 reads the newest row for its own Databricks table:

```sql
SELECT * FROM "bdb_ifc_synthetic_data_test"."batch_recon"
WHERE lower(replace(target_table_name, '`', '')) = ?
      -- 'sit_cds_snsvc0080860_prepared_db.bdb_ifc_synthetic_data_test.bdp_corp_ifc_trigger_9'
ORDER BY last_modified_ts DESC
LIMIT 1
```

The match ignores backticks and case, so `` `sit_cds_snsvc0080860_prepared_db`.bdb_ifc_synthetic_data_test.BDP_Corp_IFC_Trigger_9 ``
and the same name without backticks both match. A rerun of the upstream job **appends a new row**
with the current timestamp, so the newest row is always upstream's current verdict.

| Newest row | Outcome | Exit | Meaning |
|---|---|---|---|
| none for the trigger | `UPSTREAM_DATA_NOT_RECEIVED` | 22 | upstream never wrote a recon row for this trigger |
| `last_modified_ts` not in the current month | `UPSTREAM_PROCESSING_NOT_DONE` | 22 | upstream has not processed this month |
| `status` is `FAILED` | `UPSTREAM_MODEL_FAILED` | 21 | the dbt model itself did not run |
| `status` is `RECON_FAILED` | `UPSTREAM_JOB_FAILED` | 21 | the model ran, but upstream's reconciliation failed |
| `SUCCESS` but `source_count != target_count` | `UPSTREAM_COUNT_MISMATCH` | 21 | upstream should have said `RECON_FAILED` and did not |
| `SUCCESS`, both counts zero | `NO_DATA_THIS_MONTH` | 0 | a genuine month with no data — **a success**, see below |
| `SUCCESS`, counts equal and non-zero | proceeds and publishes | — | |
| table cannot be queried (missing, `AccessDenied`, query failed or timed out) | `UPSTREAM_RECON_UNREADABLE` | 20 | the recon table cannot be read |
| row missing `status` / `last_modified_ts`, non-numeric counts, or a status outside the three | `UPSTREAM_RECON_UNREADABLE` | 14 | the row cannot be trusted |

**Every block says what went wrong.** The reason in the alert, the run summary and the manifest
quotes the row itself — for example:

```
Upstream job failed for 2026-10: the dbt model for `sit_cds_snsvc0080860_prepared_db`.bdb_ifc_synthetic_data_test.BDP_Corp_IFC_Trigger_9
did not run (status=FAILED, source_count=0, target_count=0, error_record_count=0, model_name=…, job_run_id=…, batch_id=…, last_modified_ts=…)
```

The summary and manifest also carry the whole row, the recon table, the target and the Athena query
execution id.

Exit codes are shared — two use 22 (`TED_MISSING_SOURCE_DATA`), three use 21 (`TED_JOB_FAILURE`) —
because those are the catalogue scenarios they belong to. The **outcome name** is what tells them
apart in the stopped-task record and the run summary, so the names are deliberately kept
distinct. A block is a reported failure, not a silent skip: non-zero exit, plus the same alert a
run failure raises, because nobody reads the logs on a monthly schedule.

**A genuine empty month is not a failure.** When upstream reconciles `SUCCESS` with zero source and
target records there is nothing to publish, and that *is* the month delivered. The run stops at
the recon check as usual, but it exits `0`, raises no RTB alert, records `SUCCESS` with
`records_processed = 0` in the run marker file (so the rest of the window stands down), and sends
the [batch completion notification](#the-batch-completion-notification) with
`No_Of_Messages_Produced = 0`.

**The current month, not the run month.** Upstream writes its recon row when it runs, so the row
has to be from the month the task runs in — also on a reprocess with `IFC_RUN__MONTH`, whose run
month is earlier. `last_modified_ts` is UTC; the current month is taken from the UK date, which
only differs in the hour around midnight at a month end, when no run is scheduled.

**Stopping is the safe direction.** Because the contract is exactly three statuses, any other value
is treated as an untrusted row rather than another outcome to interpret — there is no state in
which an unknown status is a green light. Counts that disagree are upstream's own definition of a
failed reconciliation, so a `SUCCESS` carrying them is a contradiction and the run stops; that is
the backstop for upstream having missed it.

**Nothing downstream is touched when the gate blocks.** Every outcome except "proceeds" stops at the
recon check: no preflight, no Kafka, no BSP, and the trigger table is never queried.

**Ordering.** It runs *after* the invocation gate's weekend and already-delivered skips, and
before preflight, Kafka, the source and the marker claim. Those skips are days the run is not
meant to happen at all; checking upstream on them would alert six times a month for nothing.

`recon.enabled: false` (`IFC_RECON__ENABLED=false`) switches it off, and so does leaving
`recon.table` unset or the trigger out of `recon.trigger_targets` — the same convention as
`run_marker.path`, so a local run needs no recon feed. The old S3 settings (`path`,
`trigger_paths`, `file_suffixes`, …) are rejected at load, so a stale config cannot switch the gate
off silently. The task role needs the same Athena and Glue permissions as the trigger tables, on
the recon table too, plus read on its data.

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

The trigger also chooses **which table is read**: `source.trigger_tables` maps each trigger to its
own Athena table. The UAT config sets no fallback `source.table`, so a trigger without an entry
fails at startup instead of reading another trigger's data.

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

Deployment artefacts (`deploy/`, kept outside this repository):

| File | Contents |
|---|---|
| `ecs-task-definition.json` | Fargate task definition. `stopTimeout: 120` **must** exceed `run.shutdown_grace_seconds` (90), or SIGKILL wins and the drain is lost |
| `iam-task-role-policy.json` | Least-privilege task role, including an explicit **Deny** on deleting from the Trigger BDP — the connector only reads it |
| `infrastructure.json` | The run-marker file location, the three EventBridge trigger schedules, egress security group (9095/8095/BAM/CyberArk CCP), log retention |
| `athena-run-markers.sql` | Athena table and latest-state view over the run marker file |
| `cloudwatch-alarms.json` | One alarm per observable scenario, each naming the catalogue scenario it detects |

One deployment shape: an ECS RunTask per invocation under EventBridge Scheduler — one schedule
per trigger, each firing on three consecutive dates (`cron(0 3 3,4,5 * ? *)` and so on) and
setting `IFC_RUN__TRIGGER`. A run without it fails. The container decides whether to process
(see [The invocation gate](#the-invocation-gate)), publishes the month, and exits. There is no
resident service mode: a resident task would re-query and republish the same month.

**Alarms use the `Maximum` statistic.** The EMF counters (`MessagesAcked`, `MessagesFailed`,
`RecordsQuarantined`, ...) are running totals for the run, re-emitted at every progress report
and again at the end, so `Sum` over a period adds the same records up several times. `Maximum`
is the run's final value.

Health endpoints on `:8080` — `/health/live` (restart me, used by the container health check) and
`/metrics`. A run loop that stops checking in for five minutes reports **not live**, so a task
wedged on a stuck socket is restarted rather than left publishing nothing.

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
recon confirming zero is still withheld — only the recon row can say a month is genuinely
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
| CyberArk client certificate and key | Secrets `/ifc/bsp-event-processor/cyberark/client-cert` and `.../private-key` exist but are empty; the CCP query comes from the product template's `CYBERARK_*` variables |
| Confirmed IFC CDD topic name and registry subject | `tc01_fncmtrgrbb_ifc_tbb_kyc_refresh` assumed from the topic table |
| Kafka ACLs for the producer principal on the topic | Needed; preflight distinguishes a missing ACL from a missing topic |
| **Tokenisation policy names** for account fields | Only `DPASS_POLICY_NAME` (Client Relationship Owner Name) is confirmed on Confluence; `POLICY_ACCOUNT` in `utility/trigger_payload.py` is a placeholder |
| Timestamp format agreement with TBB | See drift item 4 |
| Athena workgroup, and the task role's Athena / Glue / S3 / Lake Formation permissions | `workgroup: primary` is a placeholder; the role needs `athena:StartQueryExecution`, `GetQueryExecution`, `GetQueryResults`, `StopQueryExecution`, `GetTableMetadata`, Glue `GetTable`/`GetPartitions`, read on the table data, read/write on `s3://sit1-logs-corpdeng-509153454187-eu-west-1/athena_output/`, and `SELECT` if Lake Formation governs the tables |
| `date_of_request` text format | A `string` column; the payload publishes its first ten characters, which is the date for ISO text (`2026-08-10 02:15:04`). Confirm upstream writes ISO |
| Internal package index reachable from the image build, for `bsp_python_client` | Required; it is an ordinary pip requirement |
| SNS topic for RTB alerting | `notifications.sns_topic_arn` is null; alerts currently log only |
| SNS topic for the TBB batch-completion event | `notifications.batch_sns_topic_arn` is null; the event currently logs only. Confirm the topic and the `Trigger_Originating_BU` value with TBB |
