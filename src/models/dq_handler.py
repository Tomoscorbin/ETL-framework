from argparse import ArgumentParser
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from databricks.labs.dqx.engine import DQEngine  # type: ignore
from databricks.sdk import WorkspaceClient
from src import settings
from src.enums import DQFailureSeverity
from src.logger import LOGGER

if TYPE_CHECKING:
    from src.models.table import DeltaTable


class DQHandler:
    """
    Runs Databricks Labs DQx checks on a DataFrame, writes failures to a Delta
    DQ table, and aborts the pipeline when ERROR-level failures occur.
    """

    dq_engine = DQEngine(WorkspaceClient())
    dq_table_name = f"{settings.CATALOG}.metadata.data_quality_checks"

    def __init__(self, delta_table: "DeltaTable", dataframe: DataFrame):
        self.delta_table = delta_table
        self.dataframe = dataframe
        self.rules = delta_table.rules

    def _apply_checks(self) -> DataFrame | None:
        _, quarantine_df = self.dq_engine.apply_checks_and_split(self.dataframe, self.rules)
        return quarantine_df

    def _get_failures(self, quarantine_df: DataFrame, severity: str) -> DataFrame:
        severity_stripped = severity.replace("_", "")
        return (
            quarantine_df.select(F.explode(F.col(severity)).alias("failure"))
            .select(
                F.col("failure.name").alias("check_name"),
                "failure.columns",
                "failure.function",
                "failure.run_time",
            )
            .distinct()
            .withColumns(
                {
                    "severity": F.lit(severity_stripped),
                    "table_name": F.lit(self.delta_table.full_name),
                }
            )
        )

    def _save_checks_to_table(self, summary_df: DataFrame) -> None:
        summary_df.write.saveAsTable(name=self.dq_table_name, mode="append", format="delta")

    def _add_metadata_columns(self, dq_checks_df: DataFrame) -> DataFrame:
        job_id, run_id = self._get_job_ids()
        return dq_checks_df.withColumns(
            {
                "job_id": F.lit(job_id).cast(T.LongType()),
                "run_id": F.lit(run_id).cast(T.LongType()),
                "date": F.current_date(),
            }
        )

    def _handle_warnings(self, quarantine_df: DataFrame) -> None:
        warnings_df = self._get_failures(quarantine_df, DQFailureSeverity.WARNINGS)
        if not warnings_df.isEmpty():
            LOGGER.warning(f"DQ warning(s) detected for {self.delta_table.full_name}.")
            warnings_df = self._add_metadata_columns(warnings_df)
            self._save_checks_to_table(warnings_df)


    def _handle_errors(self, quarantine_df: DataFrame) -> None:
        errors_df = self._get_failures(quarantine_df, DQFailureSeverity.ERRORS)
        if not errors_df.isEmpty():
            errors_df = self._add_metadata_columns(errors_df)
            self._save_checks_to_table(errors_df)
            raise RuntimeError(f"DQ ERROR(s) detected for {self.delta_table.full_name}.")


    def apply_and_save_checks(self):
        """Runs data quality checks on the DataFrame and handles any failures."""
        if not self.rules:
            return

        quarantine_df = self._apply_checks()
        self._handle_warnings(quarantine_df)
        self._handle_errors(quarantine_df)


    @staticmethod
    def _get_job_ids() -> tuple[Any, Any]:
        p = ArgumentParser()
        p.add_argument("--job_id", dest="job_id", default=None)
        p.add_argument("--run_id", dest="run_id", default=None)
        args, _ = p.parse_known_args()

        return args.job_id, args.run_id
