"""Entry point for running all DDL operations in the project."""

import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[1]))

from pyspark.sql import SparkSession

import src
from src.ddl.utils import ensure_all_delta_tables


def run_ddl(spark: SparkSession) -> None:
    """Orchestrate DDL operations."""
    ensure_all_delta_tables(package=src, spark=spark)


if __name__ == "__main__":
    from src.runtime import spark

    run_ddl(spark)
