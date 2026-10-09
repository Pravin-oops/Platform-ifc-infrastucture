# IFC Trigger Connector

Runs on **AWS ECS** once a month per trigger and publishes the **IFC CDD trigger events
(Triggers 8, 9 and 21)** from their Athena (Iceberg) tables onto the **BSP Trigger Backbone**
Kafka topic. Each run reads one month, publishes it, records the outcome and exits with a code
that names what happened.

## Contents

1. [SIT run: ECS container values](#1-sit-run-ecs-container-values)
2. [SIT run: end-to-end flow](#2-sit-run-end-to-end-flow)
3. [What a run leaves behind](#3-what-a-run-leaves-behind)
4. [Configuration](#4-configuration)
5. [Message contract](#5-message-contract)
6. [The run gate](#6-the-run-gate)
7. [The upstream reconciliation gate](#7-the-upstream-reconciliation-gate)
8. [Failure handling and exit codes](#8-failure-handling-and-exit-codes)
9. [Batch completion notification (SNS)](#9-batch-completion-notification-sns)
10. [Reprocessing a past month](#10-reprocessing-a-past-month)
11. [Build, run and test](#11-build-run-and-test)
12. [Repository layout](#12-repository-layout)
13. [Open items](#13-open-items)

---

## 1. SIT run: ECS container values

The image's `ENTRYPOINT` is `scripts/main_ecs.py`. No config file name is passed in: the
environment picks it.

| Variable | Required | Set by | SIT value / purpose |
|---|---|---|---|
| `IFC_APP__ENVIRONMENT` | **yes** | task definition | `SIT` — loads `utility/connector_config_sit.yaml`, which names `utility/bsp_sit_config.yaml` |
| `IFC_RUN__TRIGGER` | **yes** | EventBridge Scheduler override | `TRIGGER_8`, `TRIGGER_9` or `TRIGGER_21` (also `trigger 9`, `T9`, `9`) |
| `CYBERARK_ENABLED`, `CYBERARK_CCP_URL`, `CYBERARK_APP_ID`, `CYBERARK_SAFE`, `CYBERARK_ACCOUNT` | for CyberArk | ECS product template | the CCP query for the BSP system account; with `CYBERARK_ENABLED` unset or `false`, `BSP_USERNAME` / `BSP_PASSWORD` must be set instead |
| `BAM_URL`, `BAM_NAME` | **yes** (SECURE registry) | task definition | read by the BSP client to fetch the BAM token |
| `AWS_REGION` | **yes** | task definition | `eu-west-1` |
| `IFC_RUN__MONTH`, `IFC_RUN__FORCE` | no | one-off RunTask | reprocess a past month — see [section 10](#10-reprocessing-a-past-month) |
| `IFC_APP__LOG_LEVEL` | no | task definition | `INFO` (default) or `DEBUG` for per-endpoint and per-row detail |
| `IFC_ENVELOPE__TOKENISED_ENVIRONMENTS` | no | task definition | e.g. `SIT` to declare the owner-name tokenisation policy in SIT |
| `IFC_<SECTION>__<KEY>` | no | task definition | overrides any other config value, e.g. `IFC_SCHEMA_REGISTRY__SECURE__URL` |

`.env` is not copied into the image (`.dockerignore`). Where an `.env` is present (local runs) it
only fills variables that are not already set, so a task value always wins.

With `IFC_APP__ENVIRONMENT=SIT` the run resolves to:

| Setting | SIT value |
|---|---|
| Kafka | `tc01_fncmtrgrbb_ifc_tbb_kyc_refresh`, 5 brokers on `:9095`, `SASL_SSL` + `OAUTHBEARER` (BAM token through the BSP `oauth_cb`) |
| Schema Registry | `SECURE`: `ldndsr000010177…:8095` and `gbrdsr000014551…:8095`, BAM bearer token, subject `<topic>-value` |
| Trigger tables | `bdb_ifc_synthetic_data_test.bdp_corp_ifc_trigger_8`, `_9`, `_21` |
| Recon table | `bdb_ifc_synthetic_data_test.batch_recon` |
| Athena | workgroup `primary`, results `s3://sit1-logs-corpdeng-509153454187-eu-west-1/athena_output/` |
| Run marker file | `s3://sit1-pre-cds01-…/ifc-bdp-audit-recon-json/run-markers/run_markers.json` |
| Audit bucket | `s3://sit1-pre-cds01-…/ifc-bdp-audit-recon-json/audit-bucket` (manifests, quarantine, payloads) |
| Root CA | Secrets Manager `/ifc/bsp-event-processor/kafka/ca-bundle` → `/tmp/ifc-certs/CARoot.pem` |
| Service number | `SNSVC0084378` (`triggerOriginatingSystem`, first part of every trigger ID) |
| Tokenisation policy | not declared (only `PROD` is in `tokenised_environments`) |

---

## 2. SIT run: end-to-end flow

```
 ECS RunTask (EventBridge Scheduler)
   IFC_APP__ENVIRONMENT=SIT, IFC_RUN__TRIGGER=TRIGGER_8
                 │
                 ▼
 ① Load config ─── connector_config_sit.yaml + CYBERARK_* + IFC_ overlay ──✗──► exit 10
                 │
                 ▼
 ② Run gate ────── run marker file (S3) ── month delivered / weekend ──────────► NOT RAN, exit 0
                 │
                 ▼
 ③ Recon gate ──── batch_recon (Athena) ── not ready / failed ─────────────────► NOT RAN, exit 21/22/14/20
                 │                      └─ SUCCESS with 0 records ─────────────► SUCCESS, SNS (0), exit 0
                 ▼
 ④ Preflight ───── table · CA · CyberArk · DNS/TCP · BAM · schema · producer ──✗──► FAILURE, exit 12/14/34/40…
                 │
                 ▼
 ⑤ Read ────────── Athena: rows WHERE business_date = last day of previous month
                 │
                 ▼
 ⑥ Per row ─────── build envelope ─ validate ─ serialise ─ size guard ─ publish ──► rejected → S3 quarantine
                 │
                 ▼
 ⑦ Close ───────── flush ─ reconcile ─ quality gate ─ manifest ─ RTB alert on failure
                 │
                 ▼
 ⑧ Finish ──────── SNS batch completion ─ run marker SUCCESS/FAILURE ─ "Run summary" ─ exit code
```

### ① Load the configuration

- `IFC_APP__ENVIRONMENT=SIT` selects `utility/connector_config_sit.yaml` (map:
  `ENVIRONMENT_CONFIGS` in `utility/connector_config.py`). Unset, unknown (anything but `DEV`,
  `SIT`, `PROD`) or missing from the image → the container stops, exit `10`.
- Layers, lowest first: model defaults → the YAML → `CYBERARK_*` → `IFC_` variables.
- `IFC_RUN__TRIGGER` is resolved: it picks the trigger table, the recon target and the published
  sub-type. Missing or unmapped → stops, exit `10`.
- Logs `Connector starting on ECS: trigger=… table=… topic=… schema_registry_mode=SECURE … environment=SIT`.

### ② Run gate — should this date run at all?

Reads the run marker file, in this order ([section 6](#6-the-run-gate)):

1. `IFC_RUN__FORCE=true` → proceed.
2. This trigger's run month already has `SUCCESS` → append `NOT RAN`, exit `0`.
3. Saturday or Sunday (Europe/London) → append `NOT RAN`, exit `0`.

Nothing else is touched on a skip: no Athena, no BSP, no Kafka.

### ③ Upstream reconciliation gate — did upstream produce this month?

Queries the newest `batch_recon` row whose `target_table_name` is this trigger's
([section 7](#7-the-upstream-reconciliation-gate)). It must be from the **current** month, status
`SUCCESS`, with `source_count = target_count`.

- Not ready, failed or untrusted → `NOT RAN`, RTB alert, non-zero exit.
- `SUCCESS` with both counts zero → a delivered empty month: `SUCCESS` with 0 records, SNS
  batch event with 0 messages, exit `0`.
- Otherwise → logs `Upstream reconciliation passed` and continues.

### ④ Preflight and connection set-up

The health server starts on `:8080` and the SIGTERM handler is installed. Then, stopping at the
first blocking failure:

| Step | What happens | Fails as |
|---|---|---|
| Trigger table | Glue `GetTableMetadata` on the trigger's table (no data scanned) | `14` Trigger BDP read |
| Root CA | read from Secrets Manager, written to `/tmp/ifc-certs/CARoot.pem` | `40` / `12` |
| CyberArk | when enabled: client cert + key from Secrets Manager → CCP → `BSP_USERNAME` / `BSP_PASSWORD` | `40` / `12` |
| BSP client | loads `utility/bsp_sit_config.yaml`, applies `kafka.overrides` and the connector's delivery settings | `40` / `10` |
| Network | DNS + TCP to every broker (`:9095`) and registry node (`:8095`); one reachable broker and one registry node are enough | `12` network |
| BAM token | fetched for the SECURE registry; `exp` tracked and refreshed before expiry | `40` authentication |
| Schema | latest version of `<topic>-value`, tried node by node; the bundled `utility/schema.json` must not drift in a blocking way | `34` registry / `20` schema |
| Producer | created, then waits until the BSP `oauth_cb` has supplied a SASL token | logged at ERROR; the publishes then fail |
| Topic metadata | off by default; `IFC_RESILIENCE__PREFLIGHT_METADATA_ENABLED=true` fails fast on a missing topic or ACL | `33` / `41` |

At INFO this is two lines:

```
Preflight passed in 812 ms: source:readable ok, dns:kafka 5/5, tcp:kafka 5/5, dns:schema_registry 2/2, tcp:schema_registry 2/2, auth:bam_token ok, schema_registry:subject ok
Kafka target ready: topic=tc01_fncmtrgrbb_ifc_tbb_kyc_refresh schema_registry_mode=SECURE schema_id=… connection=BSP
```

### ⑤ Read the month

One Athena query against the trigger's table. A run in October 2026 reads September's rows:

```sql
SELECT * FROM "bdb_ifc_synthetic_data_test"."bdp_corp_ifc_trigger_8"
WHERE CAST("business_date" AS DATE) = CAST('2026-09-30' AS DATE)
ORDER BY "date_of_request", "counterparty_csid_sds"
```

Results are paged (1,000 rows a page) and typed back (`bigint` → int, `date` → date), so memory
stays flat. A query that cannot run → `SOURCE_UNREADABLE`, exit `14`. No rows → `ZERO_RECORDS`,
exit `22`.

### ⑥ Each row

1. **Build the envelope** ([section 5](#5-message-contract)): eight payload fields, sequence number,
   trigger ID.
2. **Validate** against the bundled Avro schema.
3. **Serialise** in Confluent wire format (`0x00` + schema id + Avro body).
4. **Size guard**: key + value must fit `kafka.max_message_bytes` (819,200).
5. **Publish** with the trigger ID as key; retried with jittered backoff up to 5 times, behind a
   circuit breaker (20 consecutive failures).

A row that fails steps 1–4 is **quarantined** to S3 and the run continues. At INFO the first row
of each problem is logged once:

```
Record rejected: column=Date of Request (date_of_request) check=not a valid DATE row=12 … reason=…
```

Every 500 rows the run reports progress (health heartbeat, EMF metrics).

### ⑦ Close the batch

1. **Flush** the producer (`kafka.flush_timeout_seconds`, 120).
2. **Reconcile**: `rows read = published + quarantined`, and every publish acknowledged. If not →
   `RECONCILIATION_FAILED`, exit `23`.
3. **Quality gate**: more than 5% of rows quarantined → `QUALITY_GATE_FAILED`, exit `20`, with the
   breakdown by column and check:

   ```
   Quarantine summary: 3 of 150 records rejected: Date of Request (date_of_request) not a valid DATE x2; Client Region (region) missing x1
   ```
4. **Manifest** written to the audit bucket; on any failure, an **RTB alert** to
   `notifications.sns_topic_arn`; final EMF metrics.
5. Logs `Kafka publish summary: … acked=… quarantined=…` and `Run finished: outcome=… exit_code=…`
   (with `reason=…` on failure).

### ⑧ Finish

- **SNS batch completion** to `notifications.batch_sns_topic_arn`, only when the outcome is
  `SUCCESS` and at least one message was acknowledged ([section 9](#9-batch-completion-notification-sns)).
- **Run marker**: `SUCCESS` with the acknowledged count when exit is `0` and something was
  delivered; otherwise `FAILURE`, so the next date in the window retries.
- Last log line: `Run summary: outcome=SUCCESS exit_code=0 topic=… acked=… sns_batch_notification=SENT | {…}`.
- The container exits with the run's code.

A SIGTERM at any point stops intake, flushes what is in flight within
`run.shutdown_grace_seconds` (90) and exits `75`; the marker records `FAILURE`.

---

## 3. What a run leaves behind

| Where | What |
|---|---|
| Run marker file (S3, JSON Lines) | one line per invocation: `SUCCESS`, `FAILURE` or `NOT RAN`, with the reason |
| `<audit.bucket>/manifests/run_date=YYYYMMDD/<run_id>.json` | run identity, config in force, counters, offsets, preflight report, what was read (table, business date, query id), quarantine keys, classified failure |
| `<audit.bucket>/quarantine/run_id=…/scenario=…/<trigger_id>.json` | each rejected row with the reason and enough to replay it |
| `<audit.bucket>/payloads/event_date=…/trigger_id=…/payload.avro` | every published record's bytes (`audit.write_payloads`) |
| Kafka topic | the month's trigger events |
| SNS `batch_sns_topic_arn` | the batch completion event for TBB |
| SNS `sns_topic_arn` | the RTB failure alert (subject routes by scenario) |
| CloudWatch Logs / EMF | JSON log lines and run metrics (`MessagesAcked`, `RecordsQuarantined`, `QueueDepth`, …) |
| ECS stopped-task record | the exit code ([section 8](#8-failure-handling-and-exit-codes)) |

Gate stops (run gate, recon gate) and start-up failures still write a manifest, stage `RUN_GATE`,
`RECON_GATE` or `STARTUP`. Alarms on the EMF counters should use the `Maximum` statistic: the
counters are running totals re-emitted during the run.

---

## 4. Configuration

### Per environment

| `IFC_APP__ENVIRONMENT` | Connector config | BSP client config | Kafka | Schema Registry |
|---|---|---|---|---|
| `DEV` | `utility/connector_config_dev.yaml` | `utility/bsp_dev_config.yaml` | `PLAINTEXT` `:9092`, DEV-only listener, no token | `DEV`, schema id `1299` pinned, no token |
| `SIT` | `utility/connector_config_sit.yaml` | `utility/bsp_sit_config.yaml` | `SASL_SSL` `:9095`, BAM token | `SECURE` `:8095`, BAM token |
| `PROD` | `utility/connector_config_prod.yaml` | `utility/bsp_prod_config.yaml` | not yet in the repository | |

Each connector config fixes its `app.environment`, its `kafka.bsp_config_path` and its
`schema_registry.mode`. The DEV file uses SIT's AWS locations until DEV has its own.

### Overrides

Any setting can be overridden per task with `IFC_<SECTION>__<KEY>` (`true`/`false`, numbers and
JSON are coerced; nested blocks use more `__`, e.g. `IFC_SCHEMA_REGISTRY__SECURE__URL`). Values
that need confirming are marked `CONFIRM` in the YAML.

| Variable | Effect |
|---|---|
| `IFC_SCHEMA_REGISTRY__SECURE__URL` | registry nodes, comma-separated (`https://a:8095,https://b:8095`) or a JSON list; tried in order, failing over to the next |
| `IFC_ENVELOPE__TOKENISED_ENVIRONMENTS` | `SIT`, `DEV,PROD`, …; replaces the file's list; empty leaves it to the file |
| `IFC_KAFKA__DEBUG` | librdkafka trace, e.g. `security,broker,protocol`; `all` and `conf` are refused |
| `IFC_RESILIENCE__MAX_QUARANTINE_RATIO` | quality-gate tolerance (default `0.05`) |
| `IFC_RECON__ENABLED` | `false` switches the recon gate off |

### Secrets

None are configuration. The BSP system-account credential comes from **CyberArk CCP** at runtime,
authenticated with a client certificate and key read from Secrets Manager
(`/ifc/bsp-event-processor/cyberark/client-cert`, `…/private-key`). The ECS product template sets
the CCP query:

| Variable | Fills |
|---|---|
| `CYBERARK_ENABLED` | `cyberark.enabled` (`false` in the YAML; then `BSP_USERNAME` / `BSP_PASSWORD` come from the environment) |
| `CYBERARK_CCP_URL` | `cyberark.base_url`, the full `…/AIMWebService_certs/api/Accounts` URL |
| `CYBERARK_APP_ID`, `CYBERARK_SAFE`, `CYBERARK_ACCOUNT` | `app_id`, `safe`, `object` |

The Barclays root CA is never committed or baked into the image: it is read from Secrets Manager at
start (`ca_certificate.secret_id`) and written to `/tmp/ifc-certs/CARoot.pem`, which both
`ssl.ca.location` (BSP config) and `schema_registry.ca_location` point at.

---

## 5. Message contract

### Envelope (`utility/schema.json`, `TriggerBackboneTopicSchema`)

| Field | Value |
|---|---|
| `triggerID` (also the Kafka key) | `{service number}_KYCRefresh_{triggerSubType}_{timestamp}_{sequenceNumber}` |
| `triggerType` | `KYCRefresh` |
| `triggerSubType` | `TRIGGER_8` → `NewHRCRelationship`, `TRIGGER_9` → `AccountInactivity`, `TRIGGER_21` → `MultipleTMSARs` |
| `timestamp` | last instant of the business month, e.g. `2026-09-30T23:59:59.999999999Z` |
| `triggerPostingTimestamp` | when the record is built, RFC 3339 UTC with nanoseconds |
| `sequenceNumber` | position in the batch, `1, 2, 3…` across all customers |
| `triggerOriginatingSystem` | service number by environment: DEV `SNSVC0084379`, SIT `SNSVC0084378`, PROD `SNSVC0084373` |
| `triggerOriginatingBU` | `UK-C` |
| `idType` / `idSystem` | `Customer` / `UK-C CRIME` |
| `idValue` | `counterparty_csid_sds` as a string; a row without one is quarantined |
| `upstreamTriggerID` | null |
| `payload` | JSON string: the eight fields below |

Example trigger ID: `SNSVC0084378_KYCRefresh_NewHRCRelationship_2026-09-30T23:59:59.999999999Z_1`.
The `order_by` columns fix the row order, so a re-run of the same data reproduces the same IDs.
The connector does not de-duplicate: a re-run republishes the month. Records carry no Kafka
headers.

### Payload

The `payload` is a JSON array of
`{"fieldName", "fieldValue", "fieldEncryptionPolicy", "fieldDataType"}`, the same eight fields for
every trigger, defined once in `_payload_fields()` in `utility/trigger_definitions.py`:

| # | `fieldName` | Type | Source column | Max length | Notes |
|---|---|---|---|---|---|
| 1 | `Date of Request` | `DATE` | `date_of_request` | 10 | sent as `YY-MM-DD` (`2026-09-10…` → `26-09-10`); must start with an ISO date |
| 2 | `Counterparty Full Legal Entity Name` | `STRING` | `counterparty_full_legal_entity_name` | 100 | |
| 3 | `Counterparty SDS ID / CSID` | `STRING` | `counterparty_csid_sds` | 11 | also the envelope's `idValue` |
| 4 | `Client Relationship Owner Name` | `STRING` | `client_relationship_owner_name` | 50 | policy `UK_TOK_AC_L0R0_UNC_DE` only in tokenised environments (PROD) |
| 5 | `Client Relationship Owner BRID` | `STRING` | `client_relationship_owner_brid` | 10 | |
| 6 | `Client Relationship Owner Business Unit` | `STRING` | `client_relationship_owner_business_unit` | 20 | defaults to `UK Corporate` |
| 7 | `Client Relationship Owner Location` | `STRING` | `client_relationship_owner_location` | 5 | defaults to `UK` |
| 8 | `Client Region` | `STRING` | `region` | 50 | |

All eight are mandatory: a missing or blank value (after the two defaults) or a value over its
length is quarantined, never truncated or sent empty. Every other field's
`fieldEncryptionPolicy` is `""`. The connector never tokenises; the owner name must arrive
already tokenised where a policy is declared.

Renaming a `fieldName` is one line in `trigger_definitions.py`, but it is a consumer-visible
change: agree it with Trigger Backbone first.

### Schema Registry

| | SECURE (SIT) | DEV |
|---|---|---|
| Registry | `secure.url` nodes, `:8095` | not contacted while `dev.schema_id` is pinned; otherwise `dev.url`, `:8082` |
| Authentication | BAM bearer token | none |
| Schema id | looked up; the bundled schema is checked for blocking drift | `1299` |

---

## 6. The run gate

EventBridge fires each trigger on **three consecutive dates**; the container decides whether to
work. Three consecutive dates always include a weekday.

| Trigger | Dates fired | Published as |
|---|---|---|
| `TRIGGER_8` | 3rd, 4th, 5th | `NewHRCRelationship` |
| `TRIGGER_9` | 8th, 9th, 10th | `AccountInactivity` |
| `TRIGGER_21` | 15th, 16th, 17th | `MultipleTMSARs` |

The **run marker file** (`run_marker.path`) is a JSON Lines file in S3, one line appended per
invocation and shared by all three triggers:

```json
{"trigger_id":"TRIGGER_8","year_month":"2026-10","run_date":"2026-10-05","run_status":"SUCCESS","records_processed":150,"reason":"","recorded_at":"2026-10-05T03:04:11.512Z"}
{"trigger_id":"TRIGGER_8","year_month":"2026-10","run_date":"2026-10-06","run_status":"NOT RAN","records_processed":0,"reason":"TRIGGER_8 2026-10 was already delivered","recorded_at":"2026-10-06T03:03:52.107Z"}
```

| `run_status` | When |
|---|---|
| `SUCCESS` | records were delivered (or upstream confirmed an empty month) — the only status that stops later dates |
| `FAILURE` | the connector ran and did not deliver — the next date retries |
| `NOT RAN` | weekend, already delivered, or the recon gate blocked — never changes the month's outcome |

`year_month` is the **run** month; the records it publishes belong to the month before. Appends use
a conditional S3 PUT (`If-Match` on the ETag) with retry, so two triggers finishing together do not
lose a line; keep bucket versioning on. Writing the marker is best-effort and logged loudly if it
fails.

---

## 7. The upstream reconciliation gate

The Databricks recon job appends one row per model run to `batch_recon`. The run reads the newest
row for its own trigger (`recon.trigger_targets`, matched ignoring backticks and case):

```sql
SELECT * FROM "bdb_ifc_synthetic_data_test"."batch_recon"
WHERE lower(replace(target_table_name, '`', '')) = ?
ORDER BY last_modified_ts DESC
LIMIT 1
```

| Newest row | Outcome | Exit |
|---|---|---|
| none for the trigger | `UPSTREAM_DATA_NOT_RECEIVED` | 22 |
| `last_modified_ts` not in the current month | `UPSTREAM_PROCESSING_NOT_DONE` | 22 |
| `status = FAILED` | `UPSTREAM_MODEL_FAILED` | 21 |
| `status = RECON_FAILED` | `UPSTREAM_JOB_FAILED` | 21 |
| `SUCCESS`, `source_count != target_count` | `UPSTREAM_COUNT_MISMATCH` | 21 |
| `SUCCESS`, both counts zero | `NO_DATA_THIS_MONTH` — delivered, SNS with 0 messages | 0 |
| `SUCCESS`, counts equal and non-zero | proceeds | — |
| recon table cannot be queried | `UPSTREAM_RECON_UNREADABLE` | 14 |
| row missing `status` / `last_modified_ts`, bad counts or an unknown status | `UPSTREAM_RECON_UNREADABLE` | 20 |

Every block raises the RTB alert and quotes the row (status, counts, `model_name`, `job_run_id`,
`batch_id`, `last_modified_ts`) in the alert, run summary and manifest. The recon row must be from
the **current** month even when reprocessing an earlier one, because upstream writes it when it
runs.

---

## 8. Failure handling and exit codes

| Situation | Handling |
|---|---|
| Bad row (missing field, bad date, too long, Avro mismatch, too large) | quarantined to S3; first of each column/check logged at WARNING; run continues |
| More than 5% of rows quarantined | `QUALITY_GATE_FAILED`, exit 20, with the per-column breakdown |
| Counts do not balance, or a publish was never acknowledged | `RECONCILIATION_FAILED`, exit 23 |
| Broker or registry error mid-run | retry with jittered backoff; circuit breaker after 20 consecutive failures |
| One broker or registry node down | the others are used; reported at preflight |
| BAM token expiring mid-run | refreshed `token_refresh_margin_seconds` (300) before `exp` |
| SIGTERM (deployment, scale-in) | stop intake, flush within 90 s, exit 75 |
| Memory | streamed reads and a capped producer queue (`local_queue_max_messages`) |

The exit code names the scenario, so the stopped-task record routes the incident.
`python scripts/main.py catalogue` prints the full catalogue (owner, action, handling) as JSON.

| Code | Scenario | Code | Scenario |
|---|---|---|---|
| 0 | success (also gate skips and an empty month) | 23 | producer reconciliation failure |
| 75 | drained on SIGTERM, work remaining | 24 | message too large |
| 10 | container failure (config, start-up) | 30 | high Kafka publish latency |
| 11 | out of memory | 31 | partition leader failure |
| 12 | network connectivity | 32 | broker unavailable |
| 13 / 14 | Trigger BDP write / read | 33 | topic unavailable |
| 15 / 16 | FRED processing / audit store | 34 | Schema Registry unavailable |
| 20 | schema validation (incl. quality gate) | 40 | authentication failure |
| 21 / 22 | TED job failure / missing source data | 41 | authorisation failure |

---

## 9. Batch completion notification (SNS)

A business event for the Trigger Backbone (not an alert), published to
`notifications.batch_sns_topic_arn` when a batch finishes with outcome `SUCCESS` and at least one
acknowledged message, or when upstream confirms an empty month.

| Field | Value |
|---|---|
| `Trigger_Originating_BU` | `UK-C` |
| `No_Of_Messages_Produced` | acknowledged messages (`0` for an empty month) |
| `Trigger_Sub_Type` | the published sub-type, e.g. `NewHRCRelationship` |
| `Topic_Name` | `kafka.topic` |
| `Trigger_Batch_Start_Timestamp` / `_End_Timestamp` | earliest / latest `triggerPostingTimestamp` in the batch |
| `Event_Timestamp` | when the event is raised |
| `Correlation_Id` | the run id, matching the manifest and logs |

All timestamps share one format: `2026-10-08T05:46:44.657987000Z`. `trigger_sub_type`,
`topic_name` and `correlation_id` are also SNS message attributes. With `batch_sns_topic_arn` null
(SIT today) or `batch_notifications_enabled: false`, the event is logged instead of sent. A failed
publish is logged and does not fail the run.

---

## 10. Reprocessing a past month

```bash
IFC_APP__ENVIRONMENT=SIT IFC_RUN__TRIGGER=TRIGGER_8 IFC_RUN__MONTH=2026-09 IFC_RUN__FORCE=true python scripts/main_ecs.py
```

`IFC_RUN__MONTH` is the **run** month (`2026-09` or `SEPTEMBER_2026`): the September run reads
`business_date = 2026-08-31`, stamps the August business month and records its outcome against
`2026-09`. `IFC_RUN__FORCE=true` is needed when that month is already `SUCCESS`. The weekend check
and the recon check still use today's date, so the recon row must be from the current month.

---

## 11. Build, run and test

```bash
docker build -f Docker/Dockerfile -t ifc-trigger-connector:0.0.4 .
```

The build context is the repository root. The image is built on the Barclays RHEL 8 Python 3.12
builder; `bsp_python_client` installs from the internal package index like any other requirement,
so the build must reach that index.

Run locally (AWS credentials needed for Athena, S3 and Secrets Manager):

```bash
IFC_APP__ENVIRONMENT=SIT IFC_RUN__TRIGGER=TRIGGER_8 python scripts/main_ecs.py
```

Tests:

```bash
./run_tests.sh
```

(`run_tests.ps1` on Windows.) The tests need no AWS, BSP or Kafka.

---

## 12. Repository layout

```
├── .env                         # local settings (IFC_APP__ENVIRONMENT=DEV), not in the image
├── Docker/Dockerfile            # ENTRYPOINT scripts/main_ecs.py
├── scripts/
│   ├── main_ecs.py              # ECS entry: config → run gate → recon gate → runner → SNS → marker
│   └── main.py                  # CLI: one-off run, `catalogue`
├── utility/
│   ├── connector_config.py      # typed config, environment → file, IFC_ overlay
│   ├── connector_config_dev.yaml / connector_config_sit.yaml
│   ├── bsp_dev_config.yaml / bsp_sit_config.yaml
│   ├── schema.json              # TriggerBackboneTopicSchema (Avro)
│   ├── run_gate.py              # weekend / delivered checks, run marker file
│   ├── recon_gate.py            # upstream batch_recon check
│   ├── connector_runner.py      # preflight, read, publish, reconcile, report
│   ├── kafka_factory.py         # BSP client, preflight, producer, registry
│   ├── kafka_preflight.py       # DNS/TCP/auth/registry/metadata checks
│   ├── kafka_publisher.py       # produce, delivery reports, back-pressure
│   ├── kafka_serializers.py     # Confluent wire format, size guard
│   ├── schema_registry_client.py# registry lookup with failover, drift check
│   ├── trigger_source.py        # Athena trigger-table reader
│   ├── athena_query.py          # run an Athena query, typed rows
│   ├── trigger_definitions.py   # the eight payload fields, triggers 8/9/21
│   ├── trigger_payload.py       # payload rules (types, lengths, defaults)
│   ├── tb_outcome_schema.py     # envelope and trigger ID
│   ├── auth_helper.py           # BSP client wrapper, BAM token
│   ├── cyberark_ccp_fetch.py    # CyberArk CCP credential
│   ├── ca_certificate.py        # root CA from Secrets Manager
│   ├── audit_utility.py         # manifest, quarantine, reconciliation
│   ├── failure_catalog.py       # scenarios and exit codes
│   ├── error_classifier.py      # error → scenario
│   ├── failure_notifier.py      # RTB alert
│   ├── trigger_batch_notifier.py# TBB batch completion event
│   └── health_utility.py, observability_utility.py, resilience_utility.py, …
└── tests/
```

Deployment artefacts (task definition, IAM policy, schedules, alarms, Athena DDL) live outside this
repository. The task definition's `stopTimeout` (120) must exceed `run.shutdown_grace_seconds`
(90).

---

## 13. Open items

| Item | Status |
|---|---|
| PROD configs | `connector_config_prod.yaml` and `bsp_prod_config.yaml` not yet added; `IFC_APP__ENVIRONMENT=PROD` stops at start-up until they are |
| SNS topics | `sns_topic_arn` (RTB alerts) and `batch_sns_topic_arn` (TBB event) are null in SIT: both log only |
| CyberArk | client certificate and key secrets exist but are empty; `cyberark.enabled` is `false` in the YAML |
| Topic and subject | `tc01_fncmtrgrbb_ifc_tbb_kyc_refresh` to be confirmed |
| Kafka ACLs | producer principal needs write on the topic |
| Athena workgroup and permissions | `primary` is a placeholder; the role needs Athena, Glue, S3 (table data and results) and Lake Formation `SELECT` where it applies |
| `date_of_request` format | must start with an ISO date; confirm upstream writes ISO |
| Tokenisation | confirm with the consumer that `UK_TOK_AC_L0R0_UNC_DE` is the policy, and whether PROD-ANALYTICS / PROD-PARALLEL are tokenised |
| Field renames | `Counterparty SDS ID / CSID` and `Client Region` to be agreed with Trigger Backbone |
