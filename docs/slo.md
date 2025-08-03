# Service-Level Objectives

## 1. Scope
These SLOs govern the pipeline that produces the Gold tables consumed by the dashboard.

## 2. Objectives at a Glance
| ID      | Objective        | What We Care About                                         | Hard Target                                                        |
| ------- | ---------------- | ---------------------------------------------------------- | ------------------------------------------------------------------ |
| **T-1** | **Timeliness**   | Data ready for the dashboard                               | Gold committed **≤ 09:00 Europe/London** every weekday             |
| **C-1** | **Completeness** | All expected entities and mandatory attributes are present | • 100 % business-key coverage<br>• 0 NULLs in non-nullable columns |
| **Q-1** | **Quality**      | Data that gets through is *correct*                        | • 0 duplicate primary keys<br>• 0 negative sales values            |
| **Q-2** | **Warn Triage**  | We don’t ignore "yellow" alerts                            | P95 **time-to-acknowledge ≤ 4 h**                                  |


### Precedence Rule - Quality > Timeliness
If a Quality SLO (Q-1) breaches, the run must block, even if that means missing the 09:00 cut-off. We have agreed with the stakeholders that corrupt data is worse than late data.

## 3 Timeliness (T-1)
| Item            | Spec                                                                                                                                                 |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **SLI**         | `ready_by_09 = 1` when the last Gold table commit for day **D** finishes ≤ 09:00 local; else `0`.                                                    |
| **Target**      | **≥ 99 % of UK business days** (≤ 1 miss per 30 days).                                                                                               |
| **Measurement** | Pipeline writes `ready_ts` to `sli_freshness`; a nightly query evaluates `ready_by_09`.                                                              |
| **Escalation**  | Miss without a Quality breach → Sev-2 incident, post-mortem by next business day. <br>Miss due to Quality breach → handled under Quality SLOs below. |


## 4 Completeness (C-1)
| Aspect                    | SLI                                                  | Target      | Why It Matters                                           |
| ------------------------- | ---------------------------------------------------- | ----------- | -------------------------------------------------------- |
| **Business-key coverage** | `distinct_keys_gold / distinct_keys_expected`        | **= 100 %** | Dedupe in Silver can’t drop whole orders/customers.      |
| **Mandatory attributes**  | `% rows where all non-nullable columns are non-NULL` | **= 100 %** | Guarantees we never surface partial or unusable records. |


*Expected keys live in dim_expected_keys; mandatory columns are tagged in the schema registry.*

## 5 Quality (Q-1)
| Rule                      | SLI               | Target        | Enforcement                       |
| ------------------------- | ----------------- | ------------- | --------------------------------- |
| **Duplicate PKs**         | `dupe_pk_count`   | **0 per run** | Abort run and alert.              |
| **Negative sales values** | `neg_sales_count` | **0 per run** | Abort run and alert.              |


A DQ incident is logged when either count > 0.
The Timeliness SLO is suspended; the clock starts on Time-to-Recovery (see § 6).

## 6 Warning-Level DQ Rules (Q-2)
Some rules are marked severity = “warn” (e.g., unexpected currency codes, rare out-of-range quantities). They don’t block the batch but must be reviewed.

| SLI                                          | Target                         | Window / Budget                 |
| -------------------------------------------- | ------------------------------ | ------------------------------- |
| **Time-to-acknowledge warn** (`tt_ack_warn`) | **P95 ≤ 4 h** (business hours) | Rolling 30 days; ≤ 5 % breaches |


A Teams/Slack bot tags incidents; first human “eyes” reaction timestamp is captured for tt_ack_warn.

## 7 Time-to-Recovery After DQ Block
| SLI                                           | Target                                                                  |
| --------------------------------------------- | ----------------------------------------------------------------------- |
| `ttr_minutes = ready_ts_success – fail_ts_dq` | **P95 ≤ 240 min** *and* **99 % resolved before the next 09:00 cut-off** |
