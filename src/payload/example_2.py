import os
from pyspark.sql import SparkSession


def run_spark_example_2(spark: SparkSession, data_dir: str):
    rdd = spark.sparkContext.textFile(f"file://{data_dir}/README.md")
    print(rdd.count())
