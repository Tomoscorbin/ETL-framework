from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL-DEMO").getOrCreate()
