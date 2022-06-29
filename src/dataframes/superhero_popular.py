import sys
import os
from dotenv import load_dotenv
import pandas as pd
from threading import Thread
from typing import Iterator
from pyspark.sql import SparkSession, Column as SparkColumn
from pyspark.sql.functions import (
    pandas_udf,
    explode,
    udf,
    split,
    size,
    sum as pyspark_sum,
    min as pyspark_min,
)
from pyspark.sql.types import (
    StringType,
    FloatType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)


current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from spark_config import SparkConfig


def popular_superhero(spark: SparkSession, data_dir: str):

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

    connections = (
        hero_graph.withColumn("HeroID", split("value", " ")[0])
        .withColumn("connections", size(split("value", " ")) - 1)
        .groupBy("HeroID")
        .agg(pyspark_sum("connections").alias("connections"))
    )

    # Most popular
    # This is a Row object
    most_popular_hero = connections.sort("connections", ascending=False).first()
    most_popular_hero_name = hero_names.filter(
        hero_names.HeroID == most_popular_hero["HeroID"]
    ).first()

    print(
        f"{most_popular_hero_name['HeroName']} is the most popular superhero with {most_popular_hero['connections']} co-appearances."
    )

    # Most obscure
    min_connections = connections.agg(pyspark_min("connections")).first()
    most_obscure_heros = connections.filter(
        connections.connections == min_connections[0]
    ).join(hero_names, "HeroID")
    print(f"The following characters only have {min_connections[0]} connections(s)")
    most_obscure_heros.select("HeroName").show()


if __name__ == "__main__":
    load_dotenv()
    data_dir = "/opt/bitnami/spark/data/"
    with SparkConfig("PopularSuperhero") as spark:
        popular_superhero(spark, data_dir)
