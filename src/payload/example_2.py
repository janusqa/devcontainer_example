import os

def run_spark_example_2(spark):
    rdd = spark.sparkContext.textFile("README.md")
    rdd.count()
