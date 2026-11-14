# IFC Trigger Connector — Production Runbook

Operating guide for the connector in **PROD**: what normal looks like, how to tell a run went
wrong, who owns each failure, and the procedures to recover. How the connector works is in the
[README](README.md); this document links to it rather than repeating it.

> **PROD is not deployable yet.** `utility/connector_config_prod.yaml` still holds
> `REPLACE_ME_PROD_…` placeholders and `utility/bsp_prod_config.yaml` does not exist. Complete
> [section 2](#2-go-live-readiness) before the first PROD schedule is enabled.

## Contents

1. [Service at a glance](#1-service-at-a-glance)
2. [Go-live readiness](#2-go-live-readiness)
3. [Release and rollback](#3-release-and-rollback)
4. [Monthly operation](#4-monthly-operation)
5. [Monitoring and alerting](#5-monitoring-and-alerting)
6. [Triage by exit code](#6-triage-by-exit-code)
7. [Recovery procedures](#7-recovery-procedures)
8. [Known issues and traps](#8-known-issues-and-traps)
9. [Contacts and escalation](#9-contacts-and-escalation)

---

## 1. Service at a glance

| | |
|---|---|
| What | Monthly batch: reads one month of IFC CDD trigger rows from Athena and publishes them to the BSP Trigger Backbone Kafka topic |
| Runs on | AWS ECS (one task per invocation), `eu-west-1`, started by EventBridge Scheduler |
| Image | `Docker/Dockerfile`, entrypoint `scripts/main_ecs.py`, runs as uid `10001` |
| Environment | `IFC_APP__ENVIRONMENT=PROD` → `utility/connector_config_prod.yaml` → `utility/bsp_prod_config.yaml` |
| Service number | `SNSVC0084373` (first part of every PROD trigger ID) |
| Consumers | Trigger Backbone (TBB) via Kafka; batch completion event via SNS |
| Upstream | Databricks (EDP) loads the trigger tables and writes `batch_recon` |
| Success criterion | each trigger's previous business month delivered once, within its window |

### Schedule

Each trigger fires on three consecutive dates; the first weekday that passes the gates delivers
the month and the remaining dates skip ([README §6](README.md#6-the-run-gate)).

| Trigger | Window | Published as |
|---|---|---|
| `TRIGGER_8` | 3rd – 5th | `NewHRCRelationship` |
| `TRIGGER_9` | 8th – 10th | `AccountInactivity` |
| `TRIGGER_21` | 15th – 17th | `MultipleTMSARs` |

The gates use the **Europe/London** date, so the schedules must be defined in `Europe/London`
too, otherwise a run near midnight is gated against the wrong day.

---

## 2. Go-live readiness

All must be true before enabling the PROD schedules. Owner in brackets.

**Configuration (Producer RTB)**

- [ ] Every `REPLACE_ME_PROD_…` in `connector_config_prod.yaml` replaced: trigger tables, Athena
      output bucket, topic, Schema Registry hosts, CA secret id,
      CyberArk cert/key secret ids, run marker path, audit bucket.
- [ ] `utility/bsp_prod_config.yaml` added (brokers on `:9095`, `SASL_SSL` + `OAUTHBEARER`,
      `ssl.ca.location: /tmp/ifc-certs/CARoot.pem`).
- [ ] Athena workgroup confirmed (`primary` is a placeholder).
- [ ] Recon gate: the task role can query `dp_controls_operational_metadata_prod.audit_batch_recon`,
      and its rows carry `source_count` and `target_count`. The gate matches on `model_name`
      (`BDP_Corp_IFC_Trigger_8` / `_9` / `_21`).
- [ ] `notifications.sns_topic_arn` (RTB alerts) and `notifications.batch_sns_topic_arn` (TBB batch
      event) set. With either null the event is only logged.
- [ ] `cyberark.enabled: true` (or `CYBERARK_ENABLED=true` in the task) and the client certificate
      and key secrets populated. Safe `Unx_CorpDLASP`, object `INTRANET-sysEUWENGDEV`.
- [ ] Tokenisation agreed with the consumer: PROD declares `UK_TOK_AC_L0R0_UNC_DE` on the owner
      name, and the owner name arrives already tokenised from upstream.
- [ ] `fieldName` renames (`Counterparty SDS ID / CSID`, `Client Region`) agreed with TBB.
- [ ] `date_of_request` confirmed ISO-formatted in PROD data.

**Platform (Producer RTB / BSP)**

- [ ] Topic exists; producer principal has write ACL on it (BSP).
- [ ] Subject `<topic>-value` registered and compatible with `utility/schema.json` (TBB/BSP).
- [ ] Firewall: task subnets reach every broker on `:9095` and every registry node on `:8095`.
- [ ] Task role: Athena, Glue, S3 (table data, Athena results, run marker, audit bucket), KMS for
      those buckets, Secrets Manager (CA, CyberArk cert/key), SNS publish, Lake Formation `SELECT`
      where it applies.
- [ ] Run marker bucket has **versioning on** (the marker uses conditional PUTs and versioning is
      the undo).
- [ ] Task definition `stopTimeout` = 120 (must exceed `run.shutdown_grace_seconds` = 90).
- [ ] Alarms from [section 5](#5-monitoring-and-alerting) created and routed.

**Proving run**

- [ ] The same image tag has completed a full SIT run (exit `0`, `acked` = rows read).
- [ ] First PROD window watched live by Producer RTB, with TBB told in advance.

---

## 3. Release and rollback

### Release

1. Merge to `main`; tests pass (`./run_tests.sh`, no AWS needed).
2. Bump the version in `Docker/Dockerfile` (`LABEL version`) and build from the repository root:

   ```bash
   docker build -f Docker/Dockerfile -t ifc-trigger-connector:<version> .
   ```

3. Push to the container registry; never reuse a tag.
4. Run the tag in **SIT** against a real month (force if SIT has already delivered it, see
   [7.2](#72-re-run-a-month-now)). Confirm the `Run summary` line shows `exit_code=0`.
5. Register a new PROD task definition revision with the new image; leave everything else unchanged.
6. Point the three EventBridge schedules at the new revision.
7. Release between windows (e.g. 18th – 2nd) so no scheduled run is in flight. If a task is
   running, wait for it to finish rather than stopping it.

Do **not** smoke-test in PROD by starting a task: any run that passes the gates publishes the real
month to the consumer.

### Rollback

Point the schedules back at the previous task definition revision. The connector holds no state
outside S3, so nothing else needs reverting. If a bad release published bad records, treat it as
[7.6](#76-bad-records-reached-the-topic).

---

## 4. Monthly operation

On each window date, for the trigger due that day:

1. **Did it run?** ECS stopped tasks for the cluster: one task per scheduled date, with an exit code.
2. **What happened?** Find the last log line of the task:

   ```
   Run summary: outcome=SUCCESS exit_code=0 topic=… acked=… sns_batch_notification=SENT | {…}
   ```

3. **Is the month recorded?** Read the run marker:

   ```bash
   aws s3 cp s3://<run-marker-bucket>/run-markers/run_markers.json - | grep '"TRIGGER_8"' | tail -5
   ```

   The month is delivered when the latest non-`NOT RAN` line for `trigger_id` + `year_month` is
   `SUCCESS`. `year_month` is the **run** month; the data is the month before.

| What you see | Meaning | Action |
|---|---|---|
| exit `0`, `SUCCESS`, `acked` > 0 | delivered | none |
| exit `0`, `SUCCESS`, 0 records, reason `NO_DATA_THIS_MONTH` | upstream confirmed an empty month; SNS sent with 0 | none |
| exit `0`, `NOT RAN`, "already delivered" | later date in the window | none |
| exit `0`, `NOT RAN`, "is a Saturday/Sunday" | weekend | check a later date in the window is a weekday |
| non-zero exit, `NOT RAN` or `FAILURE` | not delivered; the next date retries | triage ([section 6](#6-triage-by-exit-code)) before the next date |
| non-zero on the **last** date of the window | month missed; nothing retries automatically | [7.2](#72-re-run-a-month-now) |
| no task at all | schedule disabled, or ECS could not start the task | check the schedule and the ECS events (image pull, subnet, IAM) |

At month end, confirm all three triggers show `SUCCESS` for the month.

---

## 5. Monitoring and alerting

### Signals

| Signal | Where |
|---|---|
| Exit code | ECS stopped-task record, `containers[].exitCode` |
| RTB alert | SNS `notifications.sns_topic_arn`; subject routes by scenario; sent on every non-zero outcome |
| Run summary, errors | CloudWatch Logs (JSON lines) |
| Metrics | CloudWatch EMF, dimensions `Environment`, `Application`, `Topic`: `MessagesAcked`, `RecordsQuarantined`, `QueueDepth`, container RSS |
| Run evidence | `<audit.bucket>/manifests/run_date=YYYYMMDD/<run_id>.json` |
| Liveness | `GET :8080/health/live` (container `HEALTHCHECK`; `503` after 300 s without a heartbeat) |

### Recommended alarms

| Alarm | Condition | Severity |
|---|---|---|
| Task failed | EventBridge rule on ECS `Task State Change`, `lastStatus=STOPPED`, exit code not `0` | page Producer RTB |
| Month missed | no `SUCCESS` marker for a trigger by the day after its window closes (scheduled check) | page Producer RTB |
| Quarantine | `RecordsQuarantined` `Maximum` > 0 | ticket |
| Memory | container RSS / limit > 85% | ticket |

EMF counters are running totals re-emitted during the run: alarm on `Maximum`, never `Sum`.

### Logs Insights

```
fields @timestamp, @message
| filter @message like /Run summary|Run finished|Preflight|Upstream reconciliation|Run gate/
| sort @timestamp asc
```

```
fields @timestamp, level, @message
| filter level in ["ERROR", "WARNING"]
| sort @timestamp asc
```

---

## 6. Triage by exit code

Start from the exit code, then read the RTB alert or the manifest: both name the scenario and
quote the error. `python scripts/main.py catalogue` prints every scenario with its owner and
actions as JSON (source: `utility/failure_catalog.py`).

**Owner** is who fixes it; Producer RTB always runs the triage and informs TBB RTB.

| Exit | Scenario | Owner | First checks | Retries itself? |
|---|---|---|---|---|
| `0` | success, skip or empty month | — | see [section 4](#4-monthly-operation) | — |
| `1` | unclassified | Producer RTB | full error in the manifest `classification`; add a pattern to `utility/error_classifier.py` | next date |
| `10` | container / config | Producer RTB | `IFC_APP__ENVIRONMENT` and `IFC_RUN__TRIGGER` set and valid; config file in the image; recent deploy | next date |
| `11` / ECS `137` | out of memory | Producer RTB | RSS metric; row volume vs last month; raise task memory | next date |
| `12` | network | Producer RTB, inform BSP | preflight line names the `host:port` and DNS vs TCP: security groups, NACLs, routes, DNS | next date |
| `13` / `16` | audit write (manifest, quarantine, payloads) | Producer RTB | task role S3/KMS on the audit bucket | next date |
| `14` | source read | Producer RTB | Glue `GetTableMetadata` / Athena error in the log: table name, Lake Formation, S3/KMS, workgroup, results bucket. Also raised when `batch_recon` cannot be queried | next date |
| `20` | schema / quality gate / bad recon row | Producer RTB | `Quarantine summary` line: which column and check. Trigger 8 with `Nothing was published: … country_code missing in N row(s)` → the source table has rows without a Country Code; EDP must fix the data, nothing reached the topic. Registry drift at preflight → compare `utility/schema.json` with the subject. Recon row with missing fields → EDP | next date (same data fails again) |
| `21` | upstream job failed / recon mismatch | EDP | recon row quoted in the alert: `status`, counts, `job_run_id` | next date, once EDP re-runs |
| `22` | upstream not ready, or zero rows read | EDP | recon row from this month? Rows for `business_date` = last day of previous month? | next date |
| `23` | producer reconciliation | Producer RTB, inform BSP | `Kafka publish summary`: read vs published + quarantined, unacked deliveries; the topic may hold a partial month | see [7.3](#73-partial-publish-exit-23-30-32-75) |
| `24` | message too large | Producer RTB | quarantined record shows measured size; limit 819,200 bytes | next date (same data fails again) |
| `30` | publish latency | Producer RTB, inform BSP | ack latency metric, `QueueDepth`; BSP cluster health | see [7.3](#73-partial-publish-exit-23-30-32-75) |
| `31` | partition leader | TBB RTB / BSP | partition named in the error; usually transient | next date |
| `32` | broker unavailable | TBB RTB / BSP (raise P3) | `_TRANSPORT` / `ALL_BROKERS_DOWN`; reachability at preflight. See [known issue 8.1](#81-exit-32-on-the-first-broker-request) | see [7.3](#73-partial-publish-exit-23-30-32-75) |
| `33` | topic unavailable | TBB RTB / BSP | `kafka.topic` and overrides; topic exists in PROD | next date |
| `34` | Schema Registry unavailable | TBB RTB / BSP | which nodes failed at preflight; `schema_registry.secure.url` | next date |
| `40` | authentication | Producer RTB | CA secret present and valid; CyberArk cert/key not expired; CCP reachable; BAM token fetch error | next date (after the fix) |
| `41` | authorisation | TBB RTB / BSP | principal and topic in the error; ACL request | next date (after the fix) |
| `75` | stopped by SIGTERM, work remaining | Producer RTB | why ECS stopped it (deployment, scale-in, manual stop) | see [7.3](#73-partial-publish-exit-23-30-32-75) |

"Next date" means the run marker recorded `FAILURE` or `NOT RAN`, so the next date in the window
runs the month again on its own. If the cause is not fixed before then, the next date fails the
same way; on the last date use [7.2](#72-re-run-a-month-now).

---

## 7. Recovery procedures

All one-off runs use `aws ecs run-task` with the PROD task definition and container overrides.
Template (fill the cluster, task definition, subnets and security groups from the schedule's target):

```bash
aws ecs run-task --cluster <cluster> --task-definition <task-def>:<revision> --launch-type <launch-type> --network-configuration "awsvpcConfiguration={subnets=[<subnet>],securityGroups=[<sg>],assignPublicIp=DISABLED}" --overrides '{"containerOverrides":[{"name":"<container>","environment":[{"name":"IFC_RUN__TRIGGER","value":"TRIGGER_8"}]}]}'
```

The variables below go in the `environment` list.

### 7.1 Let the window retry

Default for any failure early in the window. Fix the cause, then let the next scheduled date run.
Nothing to do on the marker: `FAILURE` and `NOT RAN` never block a retry.

### 7.2 Re-run a month now

When the window has closed, or you cannot wait for the next date.

| Case | Variables |
|---|---|
| This month's run (data = last month), not yet delivered, weekday | `IFC_RUN__TRIGGER` only |
| This month's run on a weekend, or already `SUCCESS` | `IFC_RUN__TRIGGER`, `IFC_RUN__FORCE=true` |
| An earlier month's run | `IFC_RUN__TRIGGER`, `IFC_RUN__MONTH=YYYY-MM`, `IFC_RUN__FORCE=true` if that month is `SUCCESS` |

`IFC_RUN__MONTH` is the **run** month: `2026-09` reads `business_date = 2026-08-31`
([README §10](README.md#10-reprocessing-a-past-month)). With `IFC_RUN__FORCE=true` the recon gate
accepts the newest recon row from any month, but still blocks on `FAILED`, `RECON_FAILED` or
mismatched counts. Without force, the row must be from the current month.

**Before forcing a month that is already `SUCCESS`, agree it with TBB RTB.** The connector does not
de-duplicate: the month is republished with the same trigger IDs and TBB resolves the duplicates.

### 7.3 Partial publish (exit 23, 30, 32, 75)

Some records may have reached the topic before the run stopped.

1. Read the manifest: `acked`, the partition/offset range, and quarantine keys.
2. Tell TBB RTB the run id, the month, and the acked count.
3. Fix the cause, then re-run the whole month (7.1 or 7.2). The re-run republishes everything with
   the same trigger IDs; there is no resume-from-offset.

### 7.4 Quarantined records

Quarantined rows are at `<audit.bucket>/quarantine/run_id=…/scenario=…/<trigger_id>.json`, each
with the reason.

- Below 5% the run succeeds and the rejected rows are **not** delivered. Send the list to EDP;
  once the data is fixed upstream, the month must be re-run in full (force) to deliver them.
- Above 5% the run fails with exit `20`. Fix upstream and let the window retry. Raising the
  tolerance with `IFC_RESILIENCE__MAX_QUARANTINE_RATIO` needs sign-off from TBB, because it
  delivers an incomplete month on purpose.

### 7.5 Bypassing a gate

Only with sign-off from TBB RTB (and EDP for the recon gate), recorded in the incident.

| Need | Variable | Risk |
|---|---|---|
| Upstream is known good but `batch_recon` is late or wrong | `IFC_RECON__ENABLED=false` | publishes whatever is in the table, complete or not |
| Run on a weekend or re-deliver a month | `IFC_RUN__FORCE=true` | duplicates if the month was delivered |

### 7.6 Bad records reached the topic

The connector cannot recall messages. Stop further runs for that trigger (disable its schedule),
send TBB RTB the run id and offset range from the manifest, and agree the correction with them
(usually a corrected re-run, with TBB discarding the bad batch by run id or trigger IDs).

### 7.7 Credential and certificate rotation

| Item | Where | After rotating |
|---|---|---|
| Root CA bundle | Secrets Manager `ca_certificate.secret_id` (PEM, no private key) | nothing: read at every start |
| CyberArk client cert / key | Secrets Manager `cyberark.client_cert_secret_id` / `client_key_secret_id` | nothing: read at every start |
| BSP system-account password | CyberArk safe `Unx_CorpDLASP`, object `INTRANET-sysEUWENGDEV` | nothing: fetched at every start |

Rotate between windows. To verify, run the same image in SIT with the SIT equivalents; in PROD
the next scheduled run fails at preflight with exit `40` if a value is wrong, before anything is
read.

### 7.8 Extra diagnostics for a run

| Variable | Effect |
|---|---|
| `IFC_APP__LOG_LEVEL=DEBUG` | per-endpoint preflight and per-row detail |
| `IFC_KAFKA__DEBUG=security,broker,protocol` | librdkafka trace (`all` and `conf` are refused) |
| `IFC_RESILIENCE__PREFLIGHT_METADATA_ENABLED=true` | fails fast on a missing topic or ACL; see [8.1](#81-exit-32-on-the-first-broker-request) before using it |

### 7.9 Stopping a running task

`aws ecs stop-task` sends SIGTERM: the connector stops intake, flushes in-flight messages within
90 s and exits `75`, recording `FAILURE`. Treat the month as a partial publish ([7.3](#73-partial-publish-exit-23-30-32-75)).

---

## 8. Known issues and traps

### 8.1 Exit 32 on the first broker request

On 6 October 2026 every SIT run failed with
`KafkaError{code=_TRANSPORT,… "Failed to get metadata: Local: Broker transport failure"}`, exit `32`.
The BAM token reaches librdkafka only when the producer is polled, and the topic metadata lookup
ran first. Fixed by waiting for the token before the first request and turning the metadata check
off by default (`preflight_metadata_enabled: false`). Write-up:
`info/TRIGGER_9 Kafka Metadata Failure.docx`.

If exit `32` with `_TRANSPORT` comes back right after start, look for
`oauth_cb supplied a token: …` (good) or `The BSP oauth_cb supplied no usable token within …` /
`oauth_cb raised …` (the cause), check that `IFC_RESILIENCE__PREFLIGHT_METADATA_ENABLED` is not set to
`true`, before escalating to BSP.

### 8.2 A failed forced re-run reopens a delivered month

The marker's status is its latest non-`NOT RAN` line. If a forced re-run of a `SUCCESS` month fails,
the month now reads `FAILURE`, and any remaining scheduled date in the window re-runs and
republishes it. After a failed forced run, decide with TBB whether to let that happen or to
disable the schedule until the window closes.

### 8.3 Run marker write is best-effort

If the marker cannot be written after a successful run (logged as
`Could not record the run marker; the next invocation may repeat this month`), the next date
republishes the month. Check the marker after any such log line, and append the missing `SUCCESS`
line by hand only from a versioned copy of the file.

### 8.4 Weekend-heavy windows

A three-day window always contains a weekday, but it can contain only one (Sat, Sun, Mon). A
failure on that day leaves no automatic retry.

### 8.5 Recon gate and reprocessing

On a scheduled run the recon row must be from the current calendar month, because EDP writes it
when the job runs. `IFC_RUN__FORCE=true` drops that check and trusts the newest row, whatever month
it is from: a forced run can therefore publish data that upstream has not refreshed this month.
Check the `Forced run: accepting a reconciliation row from …` warning to see which month's row
was used.

---

## 9. Contacts and escalation

Roles come from `utility/failure_catalog.py`. Fill in the contact details before go-live.

| Team | Owns | Queue / channel | On-call |
|---|---|---|---|
| Producer RTB | the connector, AWS infra, network, credentials, data quality | _tbd_ | _tbd_ |
| TBB RTB | the topic, consumer, duplicates, partition/broker incidents | _tbd_ | _tbd_ |
| BSP Platform | brokers, Schema Registry, ACLs, BAM | _tbd_ | _tbd_ |
| EDP / Databricks | trigger tables, `batch_recon`, upstream jobs | _tbd_ | _tbd_ |

Severity follows the catalogue: `CRITICAL` (exits 10, 11, 12, 32, 34, 40, 41) blocks the month
outright; `HIGH` and `MEDIUM` usually leave the window time to retry. A month not delivered by the
end of its window is always raised to TBB RTB.
