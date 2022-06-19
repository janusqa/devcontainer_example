import os
from pyspark.sql import SparkSession


def run_spark_example_2(spark: SparkSession):
    rdd = spark.sparkContext.textFile("/opt/bitnami/spark/data/README.md")
    print(rdd.count())
