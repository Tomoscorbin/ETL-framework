from argparse import ArgumentParser
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from databricks.labs.dqx.engine import DQEngine  # type: ignore
from databricks.sdk import WorkspaceClient
from src import settings
from src.enums import DQFailureType
from src.logger import LOGGER

if TYPE_CHECKING:
    from src.models.table import DeltaTable


class DQHandler:
    """
    Runs Databricks Labs DQx checks on a DataFrame, writes failures to a Delta
    DQ table, and (optionally) aborts the pipeline when ERROR-level failures occur.
    """

    dq_engine = DQEngine(WorkspaceClient())
    dq_table_name = f"{settings.CATALOG}.metadata.data_quality_checks"

    def __init__(self, delta_table: "DeltaTable", dataframe: DataFrame):
        self.delta_table = delta_table
        self.dataframe = dataframe
        self.checks = delta_table.checks

    def _apply_checks(self) -> DataFrame | None:
        _, quarantine_df = self.dq_engine.apply_checks_and_split(self.dataframe, self.checks)
        return quarantine_df

    @staticmethod
    def _get_failures(quarantine_df: DataFrame, failure_type: str) -> DataFrame:
        return (
            quarantine_df.select(F.explode(F.col(failure_type)).alias("failure"))
            .select(
                F.col("failure.name").alias("check_name"),
                "failure.columns",
                "failure.function",
                "failure.run_time",
            )
            .distinct()
        )

    def _write_dq_summary(self, summary_df: DataFrame) -> None:
        (
            summary_df.withColumn(
                "table_name", F.lit(self.delta_table.full_name)
            ).write.saveAsTable(name=self.dq_table_name, mode="append", format="delta")
        )

    @staticmethod
    def _get_job_ids() -> tuple[Any, Any]:
        p = ArgumentParser()
        p.add_argument("--job_id", dest="job_id", default=None)
        p.add_argument("--run_id", dest="run_id", default=None)
        args, _ = p.parse_known_args()

        return args.job_id, args.run_id

    def _add_job_ids_to_summary_df(self, dq_summary_df: DataFrame) -> DataFrame:
        job_id, run_id = self._get_job_ids()
        return dq_summary_df.withColumns(
            {
                "job_id": F.lit(job_id).cast(T.LongType()),
                "run_id": F.lit(run_id).cast(T.LongType()),
                "date": F.current_date(),
            }
        )

    def apply_and_save_checks(self):
        """Runs data quality checks on the DataFrame and handles any failures."""
        if not self.checks:
            return

        quarantine_df = self._apply_checks()
        if quarantine_df.isEmpty():
            return

        errors_df = self._get_failures(quarantine_df, DQFailureType.ERRORS)
        warnings_df = self._get_failures(quarantine_df, DQFailureType.WARNINGS)
        unioned_df = errors_df.unionByName(warnings_df)

        dq_summary_df = self._add_job_ids_to_summary_df(unioned_df)
        self._write_dq_summary(dq_summary_df)

        if not warnings_df.isEmpty():
            LOGGER.warning(f"DQ warning(s) detected for {self.delta_table.full_name}.")

        if not errors_df.isEmpty():
            message = f"DQ ERROR(s) detected for {self.delta_table.full_name}."
            LOGGER.error(message)
            raise RuntimeError(message)
