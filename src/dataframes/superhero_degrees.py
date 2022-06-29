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


current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from spark_config import SparkConfig


def superhero_degrees(spark: SparkSession, data_dir: str):

    start_character_id = 5306  # 467  # Spiderman: 5306
    target_character_id = 14  # 1803  # ADAM 3.031: 14
    found = spark.sparkContext.accumulator(0)
    depth = spark.sparkContext.accumulator(0)

    def initilize(iterator):
        for i in iterator:
            yield i.assign(
                state=np.where(i["HeroID"] == start_character_id, "grey", "white"),
                depth=np.where(i["HeroID"] == start_character_id, 0, -1),
            )

    def bfs_expand(row):
        nonlocal found
        hero_id = row[0]
        connections = row[1][0]
        state = row[1][1]
        depth = row[1][2]
        new_rdd = []
        if state == "grey":
            state = "black"
            for connection in connections:
                if connection == target_character_id:
                    found.add(1)
                new_rdd_row = (connection, ([], "grey", depth + 1))
                new_rdd.append(new_rdd_row)
        new_rdd.append((hero_id, (connections, state, depth)))
        return new_rdd

    def bfs_reduce(a, b):
        connections = a[0] if len(a[0]) > len(b[0]) else b[0]
        state = (
            a[1]
            if a[1] == "black"
            else (
                b[1]
                if b[1] == "black"
                else (a[1] if a[1] == "grey" else (b[1] if b[1] == "grey" else "white"))
            )
        )
        depth = (
            a[2]
            if a[1] == "black"
            else (
                b[2]
                if b[1] == "black"
                else (a[2] if a[1] == "grey" else (b[2] if b[1] == "grey" else -1))
            )
        )

        # a[2] if a[2] > b[2] else b[2]

        return (connections, state, depth)

    schema = StructType(
        [
            StructField("HeroID", IntegerType(), False),
            StructField("HeroName", StringType(), False),
        ]
    )

    hero_names = (
        spark.read.format("csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("sep", " ")
        .schema(schema)
        .load(f"file://{data_dir}/Marvel-Names.txt")
    )

    hero_graph = (
        spark.read.format("text")
        .option("header", "false")
        .option("inferSchema", "false")
        .load(f"file://{data_dir}/Marvel-Graph.txt")
    )
    graph = (
        hero_graph.withColumn("value", trim("value"))
        .withColumn("HeroID", (split(trim("value"), " ")[0]).cast(IntegerType()))
        .withColumn(
            "connections", (split(trim("value"), " ")).cast(ArrayType(IntegerType()))
        )
        .groupBy("HeroID")
        .agg(array_distinct(flatten(collect_list("connections"))).alias("connections"))
        .withColumn("connections", array_except("connections", array("HeroID")))
    ).mapInPandas(
        initilize,
        "HeroID integer, connections array<integer>, state string, depth integer",
    )

    graph_rdd = graph.rdd.map(
        lambda x: (x["HeroID"], (x["connections"], x["state"], x["depth"]))
    )

    while found.value == 0 and depth.value < 10:
        depth.add(1)
        # scan the graph and get processed rows back
        scanned = graph_rdd.flatMap(bfs_expand, graph.schema)
        print(
            f"Processed {scanned.count()} nodes at a distance of {depth.value} from character with ID {start_character_id}."
        )
        if found.value > 0:
            print(
                f"Found the target character! From {found.value} different direction(s)."
            )
        # merge scanned back into graph
        graph_rdd = scanned.reduceByKey(bfs_reduce)

    graph = graph_rdd.map(
        lambda x: {
            "HeroID": x[0],
            "connections": x[1][0],
            "state": x[1][1],
            "depth": x[1][2],
        }
    ).toDF()

    graph.filter(
        (graph.HeroID == start_character_id) | (graph.HeroID == target_character_id)
    ).sort("depth").show()


if __name__ == "__main__":
    load_dotenv()
    data_dir = "/opt/bitnami/spark/data/"
    with SparkConfig("SuperheroDegrees") as spark:
        superhero_degrees(spark, data_dir)
