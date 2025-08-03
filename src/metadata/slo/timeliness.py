import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[2]))

from typing import Any

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from src import settings
from src.enums import Medallion, ResultState
from src.models.column import DeltaColumn
from src.models.table import DeltaTable

_CUTOFF_HOUR = 9
_JOB_NAME_TO_TRACK = "full_medallion"
_JOBS_TABLE_NAME = "system.lakeflow.jobs"
_JOB_RUNS_TABLE_NAME = "system.lakeflow.job_run_timeline"


timeliness = DeltaTable(
    table_name="timeliness",
    schema_name=Medallion.METADATA,
    catalog_name=settings.CATALOG,
    columns=[
        DeltaColumn(
            name="date",
            data_type=T.DateType(),
            is_nullable=False,
            is_primary_key=True,
        ),
        DeltaColumn(
            name="job_name",
            data_type=T.StringType(),
            is_nullable=False,
            is_primary_key=True,
        ),
        DeltaColumn(
            name="ready_by_09",
            data_type=T.BooleanType(),
            is_nullable=False,
        ),
    ],
)


def get_job_id_to_track(jobs_df: DataFrame) -> Any:
    """Get the job ID of the job we want to track."""
    # use system.lakeflow.jobs to get job ID.
    # DABs can sometimes create new jobs/IDs, so we take some recent IDs
    # and then take the most recent one
    three_days_ago = F.current_date() - F.expr("INTERVAL 3 DAY")
    is_job_to_track = F.col("name") == _JOB_NAME_TO_TRACK
    job_ran_in_last_three_days = F.col("change_time") > three_days_ago

    job_rows = (
        jobs_df.filter(is_job_to_track)
        .filter(job_ran_in_last_three_days)
        .select("job_id", "change_time")
        .collect()
    )

    last_ran_job = max(job_rows, key=lambda r: r.change_time)
    return last_ran_job.job_id


def get_latest_successful_runs(
    runs_df: DataFrame,
    job_id_to_track: str,
) -> DataFrame:
    """Return the latest successful runs of the job we want to track."""
    job_to_track = F.col("job_id") == job_id_to_track
    todays_runs = F.col("period_end_time").cast("date") == F.current_date()
    has_succesful_runs = F.col("result_state") == ResultState.SUCCEEDED
    return runs_df.filter(job_to_track).filter(todays_runs).filter(has_succesful_runs)


def derive_timeliness_metrics(latest_successful_runs_df: DataFrame) -> DataFrame:
    """Derive timeliness metrics from the latest successful runs."""
    finished_before_cutoff = F.hour("period_end_time") < _CUTOFF_HOUR
    has_any_run_before_cutoff = F.max(finished_before_cutoff)
    return latest_successful_runs_df.agg(
        F.current_date().alias("date"),
        F.lit(_JOB_NAME_TO_TRACK).alias("job_name"),
        has_any_run_before_cutoff.alias("ready_by_09"),
    )


def main(spark: SparkSession) -> None:
    """Execute."""
    jobs_df = spark.table(_JOBS_TABLE_NAME)
    runs_df = spark.table(_JOB_RUNS_TABLE_NAME)

    job_id_to_track = get_job_id_to_track(jobs_df)
    latest_successful_runs_df = get_latest_successful_runs(runs_df, job_id_to_track)
    final_df = derive_timeliness_metrics(latest_successful_runs_df)

    timeliness.merge(final_df)
