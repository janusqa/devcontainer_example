import sys
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from threading import Thread
from typing import Iterator
from pyspark.sql import (
    SparkSession,
    Column as SparkColumn,
    DataFrame as SparkDataFrame,
    Row as SparkRow,
)
from pyspark.sql.functions import (
    pandas_udf,
    explode,
    udf,
    split,
    size,
    flatten,
    sum as pyspark_sum,
    min as pyspark_min,
    collect_list,
    array_distinct,
    array_except,
    array,
    lit,
    col,
    trim,
    window,
    current_timestamp,
)
from pyspark.sql.types import (
    StringType,
    LongType,
    FloatType,
    DoubleType,
    IntegerType,
    ArrayType,
    BooleanType,
    StructField,
    StructType,
)
from pyspark.sql.functions import regexp_extract

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from spark_config import SparkConfig


def structured_streaming(spark, data_dir):

    # Monitor the logs directory for new log data, and read in the raw lines as accessLines
    accessLines = spark.readStream.text(f"{data_dir}logs")

    # Parse out the common log format to a DataFrame
    contentSizeExp = r"\s(\d+)$"
    statusExp = r"\s(\d{3})\s"
    generalExp = r"\"(\S+)\s(\S+)\s*(\S*)\""
    timeExp = r"\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]"
    hostExp = r"(^\S+\.[\S+\.]+\S+)\s"

    logsDF = accessLines.select(
        regexp_extract("value", hostExp, 1).alias("host"),
        regexp_extract("value", timeExp, 1).alias("timestamp"),
        regexp_extract("value", generalExp, 1).alias("method"),
        regexp_extract("value", generalExp, 2).alias("endpoint"),
        regexp_extract("value", generalExp, 3).alias("protocol"),
        regexp_extract("value", statusExp, 1).cast("integer").alias("status"),
        regexp_extract("value", contentSizeExp, 1)
        .cast("integer")
        .alias("content_size"),
    )

    logsDF2 = logsDF.withColumn("eventTime", current_timestamp())

    # Keep a running count of endpoints
    endpointCounts = logsDF2.groupBy(
        window(col("eventTime"), "30 seconds", "10 seconds"),
        col("endpoint"),
    ).count()

    sortedEndpointCounts = endpointCounts.orderBy(col("count").desc())

    # Display the stream to the console
    query = (
        sortedEndpointCounts.writeStream.outputMode("complete")
        .format("console")
        .queryName("counts")
        .start()
    )

    # Wait until we terminate the scripts
    query.awaitTermination()


if __name__ == "__main__":
    # from argparse import ArgumentParser

    # parser = ArgumentParser()
    # parser.add_argument("-u", "--user", type=int, default=None)
    # args = parser.parse_args()
    # if args.user:
    #     userID = int(args.user)

    load_dotenv()
    data_dir = "/opt/bitnami/spark/data/"
    with SparkConfig("StructuredStreamingPopularURLLast30s") as spark:
        structured_streaming(spark, data_dir)
