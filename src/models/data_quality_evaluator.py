from argparse import ArgumentParser
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from databricks.labs.dqx.engine import DQEngine  # type: ignore
from databricks.sdk import WorkspaceClient
from src import settings
from src.constants import DATA_QUALITY_TABLE_NAME
from src.enums import DQCriticality, Medallion
from src.logger import LOGGER

if TYPE_CHECKING:
    from src.models.table import DeltaTable


class DQError(RuntimeError):
    """Raised when a data-quality rule must block the pipeline."""

    pass


class DQEvaluator:
    """
    Runs Databricks Labs DQx checks on a DataFrame, writes failures to a
    DQ table, and aborts the pipeline when ERROR-level failures occur.
    """

    dq_table_name: str = f"{settings.CATALOG}.{Medallion.METADATA}.{DATA_QUALITY_TABLE_NAME}"

    def __init__(self, delta_table: "DeltaTable", dataframe: DataFrame):
        self.delta_table = delta_table
        self.dataframe = dataframe
        self.rules = delta_table.rules

    @cached_property
    def dq_engine(self) -> DQEngine:
        """
        Instantiate a DQEngine on first access and cache it for this handler.
        Keeps Spark from spinning up at import time.
        """
        return DQEngine(WorkspaceClient())

    def _apply_checks(self) -> DataFrame:
        _, quarantine_df = self.dq_engine.apply_checks_and_split(self.dataframe, self.rules)
        return cast(DataFrame, quarantine_df)  # DQX isn't yet typed

    def _get_failures(self, quarantine_df: DataFrame, criticality: DQCriticality) -> DataFrame:
        failure_column = criticality.quarantine_column
        return (
            quarantine_df.select(F.explode(F.col(failure_column)).alias("failure"))
            .select(
                F.col("failure.name").alias("check_name"),
                "failure.columns",
                "failure.function",
                "failure.run_time",
                F.lit(criticality).alias("criticality"),
            )
            .distinct()
        )

    def _save_checks_to_table(self, summary_df: DataFrame) -> None:
        summary_df.write.saveAsTable(name=self.dq_table_name, mode="append", format="delta")

    def _add_metadata_columns(self, dq_checks_df: DataFrame) -> DataFrame:
        job_id, run_id = self._get_job_ids()
        return dq_checks_df.withColumns(
            {
                "table_name": F.lit(self.delta_table.full_name),
                "job_id": F.lit(job_id).cast(T.LongType()),
                "run_id": F.lit(run_id).cast(T.LongType()),
                "date": F.current_date(),
            }
        )

    def _handle_warnings(self, quarantine_df: DataFrame) -> None:
        warnings_df = self._get_failures(quarantine_df, DQCriticality.WARN)
        if not warnings_df.isEmpty():
            LOGGER.warning(f"DQ warning(s) detected for {self.delta_table.full_name}.")
            warnings_df = self._add_metadata_columns(warnings_df)
            self._save_checks_to_table(warnings_df)

    def _handle_errors(self, quarantine_df: DataFrame) -> None:
        errors_df = self._get_failures(quarantine_df, DQCriticality.ERROR)
        if not errors_df.isEmpty():
            errors_df = self._add_metadata_columns(errors_df)
            self._save_checks_to_table(errors_df)
            raise DQError(f"DQ ERROR(s) detected for {self.delta_table.full_name}.")

    def apply_and_save_checks(self) -> None:
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
