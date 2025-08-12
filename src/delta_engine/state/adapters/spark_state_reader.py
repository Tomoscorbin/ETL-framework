# src/delta_engine/state/adapters/spark_state_reader.py
from typing import Dict
from pyspark.sql import SparkSession
from ..ports import (
    CatalogStateReader, SnapshotRequest, SnapshotResult,
    SnapshotWarning, Aspect, SnapshotPolicy
)
from ..states import CatalogState
from ._schema_reader import SchemaReader
from ._builder import TableStateBuilder

class SparkCatalogStateReader(CatalogStateReader):
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.schema_reader = SchemaReader(spark)
        self.builder = TableStateBuilder()

    def snapshot(self, request: SnapshotRequest) -> SnapshotResult:
        tables = request.tables
        warnings: list[SnapshotWarning] = []

        include_schema = Aspect.SCHEMA in request.aspects
        exists_map, schema_map, w1 = self.schema_reader.read_many(tables, include_schema)
        warnings.extend(w1)

        # For the first slice we skip comments/properties/PK; they’ll be added next.
        state = self.builder.assemble(
            tables=tables,
            exists=exists_map,
            schema=schema_map,
            comments={t: {} for t in tables},
            properties={t: {} for t in tables},
            primary_keys={t: None for t in tables},
        )

        if request.policy is SnapshotPolicy.STRICT and warnings:
            raise RuntimeError(f"Snapshot produced {len(warnings)} warning(s)")

        return SnapshotResult(state=state, warnings=tuple(warnings))
