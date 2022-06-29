import sys
import os
from dotenv import load_dotenv
import pandas as pd
from threading import Thread
from typing import Iterator
from pyspark.sql import SparkSession, Column as SparkColumn
from pyspark.sql.functions import pandas_udf, explode, udf
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


def popular_movies(spark: SparkSession, data_dir: str):
    def load_movie_names():
        movie_names = {}
        with open(
            "/opt/bitnami/spark/data/ml-100k/u.item",
            "r",
            encoding="ISO-8859-1",
            errors="ignore",
        ) as f:
            for line in f:
                fields = list(map(str.strip, line.split("|")))
                movie_names[int(fields[0])] = fields[1]
        return movie_names

    @pandas_udf("string")
    def movie_name_lookup(movie_ids: Iterator[pd.Series]) -> Iterator[pd.Series]:
        movie_names = movie_names_bc.value
        for movie_id in movie_ids:
            yield movie_id.map(lambda mid: movie_names[mid])

    movie_names_bc = spark.sparkContext.broadcast(load_movie_names())

    schema = StructType(
        [
            StructField("UserID", IntegerType(), False),
            StructField("MovieID", IntegerType(), False),
            StructField("rating", IntegerType(), False),
            StructField(("timestamp"), LongType(), False),
        ]
    )

    data = (
        spark.read.format("csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("sep", "\t")
        .schema(schema)
        .load(f"file://{data_dir}/ml-100k/u.data")
    )

    data.cache()
    data.createOrReplaceTempView("data")
    top_movies_ids = spark.sql(
        """
        SELECT MovieID, COUNT(MovieID) AS Popularity
        FROM data
        GROUP BY MovieID
        ORDER BY Popularity DESC
        LIMIT 10;
        """
    )
    top_movies_ids.withColumn("MovieName", movie_name_lookup("MovieID")).show()


if __name__ == "__main__":
    load_dotenv()
    data_dir = "/opt/bitnami/spark/data/"
    with SparkConfig("PopularMovies") as spark:
        popular_movies(spark, data_dir)
