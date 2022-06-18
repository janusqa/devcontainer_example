import os
from pyspark.sql import SparkSession


def run_spark_example_2(spark: SparkSession):
    try:
        with open("README.md", mode="r") as f:
            fc = f.readlines()
    except (FileNotFoundError):
        print("File not found.")
        spark.stop()
    else:
        rdd = spark.sparkContext.parallelize(fc)
        print(rdd.count())
