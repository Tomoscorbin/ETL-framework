"""Spark session initialisation used across jobs."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL-DEMO").getOrCreate()
