# Service-Level Objectives

These SLOs govern the pipeline that produces the Gold tables consumed by the weekday dashboard.

## 1. Objectives at a Glance
| ID      | Objective        | What We Care About                                       | Hard Target                                                        |
| ------- | ---------------- | -------------------------------------------------------- | ------------------------------------------------------------------ |
| **T-1** | **Timeliness**   | Data for **calendar day D1** is ready for the dashboard  | Gold committed **≤ 09:00 Europe/London** every business day        |
| **C-1** | **Completeness** | All expected entities & mandatory attributes are present | • 100 % business-key coverage<br>• 0 NULLs in non-nullable columns |
| **Q-1** | **Quality**      | Only valid data reaches consumers                        | • 0 duplicate primary keys<br>• 0 negative sales values            |
| **Q-2** | **Warn Triage**  | “Yellow” DQ warnings get human eyes promptly             | P95 **time-to-acknowledge ≤ 4 h** (business hours)                 |

### Precedence Rule — Quality > Timeliness  
If **Q-1** breaches, the run blocks even if that means missing the 09:00 cut-off.  
Stakeholders have agreed that quality is more important that freshness. In the event
of a **Q-1** breach, we formally suspend the Timeliness SLO for that day and start Time-to-Recovery tracking (see § 6).

## 2 Timeliness (T-1)
The pipeline must produce the Gold tables by 09:00 Europe/London every business day.

| Item            | Spec                                                                                                                                                 |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **SLI**         | `ready_by_09 = True` when the last Gold table commit for day **D** finishes ≤ 09:00 local; else `False`.                                             |
| **Target**      | **≥ 99 % of UK business days** (≤ 1 miss per 30 days).                                                                                               |
| **Measurement** | Pipeline writes `ready_ts` to `sli_freshness`; a nightly query evaluates `ready_by_09`.                                                              |
| **Escalation**  | Miss without a Quality breach → Sev-2 incident, post-mortem by next business day. <br>Miss due to Quality breach → handled under Quality SLOs below. |


## 3 Completeness (C-1)
Gold outputs must include all expected entities and contain no NULLs in required fields.

| Aspect                    | SLI                                                  | Target      | Why It Matters                                           |
| ------------------------- | ---------------------------------------------------- | ----------- | -------------------------------------------------------- |
| **Business-key coverage** | `distinct_keys_gold / distinct_keys_expected`        | **= 100 %** | Dedupe in Silver can't drop whole orders/customers.      |
| **Mandatory attributes**  | `% rows where all non-nullable columns are non-NULL` | **= 100 %** | Guarantees we never surface partial or unusable records. |


## 4 Quality (Q-1)
These SLOs prevent invalid data from reaching consumers. Any breach triggers a hard block and suspends the Timeliness SLO for that day.

| Rule                      | SLI               | Target        | Enforcement                       |
| ------------------------- | ----------------- | ------------- | --------------------------------- |
| **Duplicate PKs**         | `dupe_pk_count`   | **0 per run** | Abort run and alert.              |
| **Negative sales values** | `neg_sales_count` | **0 per run** | Abort run and alert.              |

A DQ incident is logged whenever these thresholds are exceeded.


## 5 Warning-Level DQ Rules (Q-2)
Some rules are defined as warn-level - they don’t block the pipeline but must be reviewed and triaged by an engineer.

Examples:
- Unexpected currency codes
- Rare values that exceed expected thresholds (e.g. abnormally high item quantities)

| SLI                                          | Target                         | Window / Budget                 |
| -------------------------------------------- | ------------------------------ | ------------------------------- |
| **Time-to-acknowledge warn** (`tt_ack_warn`) | **P95 ≤ 4 h** (business hours) | Rolling 30 days; ≤ 5 % breaches |


*(First human “eyes” reaction is captured by the alerting bot and stored for SLI calculation.)*

## 6. Time-to-Recovery (TTR)
We track how long it takes to recover from any failed pipeline run, regardless of the root cause. This helps us ensure that failures - whether due to data quality issues or infrastructure problems - are resolved fast enough to avoid user impact.

### Failure Classifications
| Type                      | Description                                                     | Timeliness SLO                            |
| ------------------------- | --------------------------------------------------------------- | ----------------------------------------- |
| **DQ failure**            | Blocked by critical data-quality rule.                          | ❌ **Suspended**                           |
| **Infra (recoverable)**   | Transient issues auto-retried by failover (e.g. cluster crash). | ✅ **Active**                              |
| **Infra (unrecoverable)** | Workspace outage, persistent network failure.                   | ⚠️ **May suspend** if documented          |
| **Code/config error**     | Bug or mis-config in our code or job params.                    | ✅ **Active**                              |
| **Upstream data missing** | Required source data not delivered or malformed.                | ⚠️ **May suspend** if upstream SLA exists |
| **Manual hold**           | Deliberate pause by engineers/stakeholders.                     | ❌ **Suspended** if pre-approved           |

### How it works
1. On failure we log fail_ts_* with the failure type.
2. On the next successful run we log ready_ts_success.
3. ttr_minutes is stored in sli_recovery and evaluated against the targets above.
