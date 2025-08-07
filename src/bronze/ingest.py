"""Utilities for ingesting raw CSV files into bronze Delta tables."""

import sys
from pathlib import Path

sys.path.append(str(Path().absolute().parents[1]))

import pyspark.sql.types as T
from pyspark.sql import SparkSession

from src import settings
from src.enums import Medallion
from src.logger import LOGGER
from src.models.column import DeltaColumn
from src.models.table import DeltaTable


_SOURCE_VOLUME = "/Volumes/source/raw/instacart"

def columns_from_schema(schema: T.StructType) -> list[DeltaColumn]:
    """Create :class:`DeltaColumn` definitions from a Spark schema."""
    return [
        DeltaColumn(field.name, field.dataType, is_nullable=field.nullable)
        for field in schema.fields
    ]


def main(spark: SparkSession, directory: str) -> None:
    """
    Ingest all CSV files within ``directory`` into bronze Delta tables.
    Each CSV file becomes a Delta table with the same name as the file stem.
    """
    LOGGER.info("Starting CSV ingestion from %s", directory)
    base_path = Path(directory)
    csv_files = list(base_path.glob("*.csv"))

    if not csv_files:
        LOGGER.warning("No CSV files found in %s", directory)
        return

    for csv_path in csv_files:
        LOGGER.info("Processing file %s", csv_path)
        df = spark.read.options(header=True, inferSchema=True).csv(str(csv_path))
        delta_table = DeltaTable(
            catalog_name=settings.CATALOG,
            schema_name=Medallion.BRONZE,
            table_name=csv_path.stem,
            comment=f"Raw {csv_path.stem} data from instacart source",
            columns=columns_from_schema(df.schema),
        )

        LOGGER.info("Writing to table %s", delta_table.full_name)
        delta_table.overwrite(df)

        LOGGER.info("Completed CSV ingestion from %s", directory)


if __name__ == "__main__":
    from src.runtime import spark

    main(spark, _SOURCE_VOLUME)
