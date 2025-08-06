"""Shared constant values used across the ETL framework."""

from typing import Final

DATA_QUALITY_TABLE_NAME: Final[str] = "data_quality_checks"
JOBS_FULL_TABLE_NAME: Final[str] = "system.lakeflow.jobs"
JOB_RUNS_FULL_TABLE_NAME: Final[str] = "system.lakeflow.job_run_timeline"
