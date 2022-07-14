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
from pyspark.ml.recommendation import ALS


current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from spark_config import SparkConfig


def movie_recommendations(spark: SparkSession, data_dir: str, userID: int):
    def load_movie_names():
        movie_names = {}

        with open(
            f"{data_dir}/ml-100k/u.item", "r", encoding="ISO-8859-1", errors="ignore"
        ) as f:
            for line in f:
                fields = line.split("|")
                movie_names[int(fields[0])] = fields[1]
        return movie_names

    # schema_movie_names = StructType(
    #     [
    #         StructField("movieID", IntegerType(), False),
    #         StructField("movieTitle", StringType(), False),
    #     ]
    # )

    # movie_names = (
    #     spark.read.format("csv")
    #     .option("charset", "UTF-8")
    #     .option("header", "false")
    #     .option("inferSchema", "false")
    #     .option("sep", "|")
    #     .schema(schema_movie_names)
    #     .load(f"file://{data_dir}/ml-100k/u.item")
    # )

    schema_movies = StructType(
        [
            StructField("userID", IntegerType(), False),
            StructField("movieID", IntegerType(), False),
            StructField("rating", IntegerType(), False),
            StructField("timestamp", LongType(), False),
        ]
    )

    movies = (
        spark.read.format("csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("sep", "\t")
        .schema(schema_movies)
        .load(f"file://{data_dir}/ml-100k/u.data")
    )

    names = load_movie_names()
    ratings = movies.select("userID", "movieID", "rating")

    print("Training recommendation model...")

    als = (
        ALS()
        .setMaxIter(5)
        .setRegParam((0.01))
        .setUserCol("userID")
        .setItemCol("movieID")
        .setRatingCol("rating")
    )

    model = als.fit(ratings)

    userSchema = StructType([StructField("userID", IntegerType(), False)])
    users = spark.createDataFrame(
        [
            [
                userID,
            ]
        ],
        userSchema,
    )

    recommendations = model.recommendForUserSubset(users, 10).collect()

    print(f"Top 10 recommenations for userID {userID}")

    for userRecs in recommendations:
        # userRecs is of form (userID, [Row(movieID, Rating), Row(movieID, rating), ...])
        myRecs = userRecs[1]
        for rec in myRecs:  # myRecs is the column of recs for the user
            movie = rec[0]  # get the movieID
            rating = rec[1]  # get rating
            movie_name = names[
                movie
            ]  # map movieID to real movie name use names object we built earlier
            print(f"{movie_name} {rating}")


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("-u", "--user", type=int, default=None)
    args = parser.parse_args()
    if args.user:
        userID = int(args.user)

        load_dotenv()
        data_dir = "/opt/bitnami/spark/data/"
        with SparkConfig("MovieSimilaritiesWithML") as spark:
            movie_recommendations(spark, data_dir, userID)
