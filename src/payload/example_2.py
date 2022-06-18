import os


def run_spark_example_2(spark):
    rdd = spark.sparkContext.textFile("file:///home/vscode/workspace/README.md")
    rdd.count()
