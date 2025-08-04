# Service-Level Objectives

These SLOs govern the pipeline that produces the Gold tables consumed by the weekday dashboard.

## 1. Objectives at a Glance
| ID      | Objective        | What We Care About                                       | Hard Target                                                        |
| ------- | ---------------- | -------------------------------------------------------- | ------------------------------------------------------------------ |
| **T-1** | **Timeliness**   | Data for **calendar day D1** is ready for the dashboard  | Gold committed **≤ 09:00 Europe/London** every business day        |
| **C-1** | **Completeness** | All expected entities & mandatory attributes are present | • 100 % business-key coverage<br>• 0 NULLs in non-nullable columns |
| **Q-1** | **Quality**      | Only valid data reaches consumers                        | • 0 duplicate primary keys<br>• 0 negative sales values            |
| **Q-2** | **Warn Triage**  | DQ warnings are reviewed promptly                        | P95 **time-to-acknowledge ≤ 4 h** (business hours)                 |


## 2 Timeliness (T-1)

The Gold tables must be ready by 09:00 Europe/London each business day.

| Item            | Spec                                                                                               |
| --------------- | -------------------------------------------------------------------------------------------------- |
| **SLI**         | `ready_by_09` is `True` if the last Gold table for day **D** commits by 09:00 local; else `False`. |
| **Target**      | **≥ 99 % of UK business days** (max 1 miss per 30 days).                                           |
| **Measurement** | Daily evaluation of `ready_by_09`.                                                                 |


## 3 Completeness (C-1)

Gold outputs must contain all expected entities and no NULLs in required fields.

| Aspect                    | SLI                                              | Target      | Importance                              |
| ------------------------- | ------------------------------------------------ | ----------- | --------------------------------------- |
| **Business-key coverage** | `distinct_keys_gold / distinct_keys_expected`    | **= 100 %** | Ensures completeness of entities.       |
| **Mandatory attributes**  | `% rows where non-nullable columns are non-NULL` | **= 100 %** | Ensures data usability and reliability. |


## 4 Quality (Q-1)
These SLOs prevent invalid data from reaching consumers. Any breach triggers a hard block and suspends the Timeliness SLO for that day.

| Rule                      | SLI               | Target        | Enforcement                       |
| ------------------------- | ----------------- | ------------- | --------------------------------- |
| **Duplicate PKs**         | `dupe_pk_count`   | **0 per run** | Abort run and alert.              |
| **Negative sales values** | `neg_sales_count` | **0 per run** | Abort run and alert.              |

The SLO tracker aggregates the `metadata.data_quality_checks` log each day. The
resulting counts of error-level failures are stored in
`metadata.quality`, allowing breaches of the Q-1 SLO to be monitored over
time.

## 5 Warning-Level DQ Rules (Q-2)

Warning-level DQ issues must be reviewed promptly.

Examples:

* Unexpected currency codes
* Rare or anomalous values

| SLI                                          | Target                         | Window / Budget                 |
| -------------------------------------------- | ------------------------------ | ------------------------------- |
| **Time-to-acknowledge warn** (`tt_ack_warn`) | **P95 ≤ 4 h** (business hours) | Rolling 30 days; ≤ 5 % breaches |

*(Human acknowledgments are manually recorded for SLI tracking.)*

