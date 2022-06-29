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


def movie_recommendations(spark: SparkSession, data_dir: str, movieID: int):
    def cosine_similarities(movie_pairs: SparkDataFrame):
        movie_pair_scores = (
            movie_pairs.withColumn("xx", movie_pairs.rating1 * movie_pairs.rating1)
            .withColumn("yy", movie_pairs.rating2 * movie_pairs.rating2)
            .withColumn("xy", movie_pairs.rating1 * movie_pairs.rating2)
        )

        movie_pair_scores.createOrReplaceTempView("movie_pair_scores")
        similarity = spark.sql(
            """
                SELECT
                    movie1,
                    movie2,
                    SUM(xy) AS numerator,
                    (SQRT(SUM(xx)) * SQRT(SUM(yy))) AS denominator,
                    COUNT(xy) AS numPairs
                FROM movie_pair_scores
                GROUP BY movie1, movie2
            """
        )

        similarity.createOrReplaceTempView("similarity")
        result = spark.sql(
            """
                SELECT
                    movie1,
                    movie2,
                    numPairs,
                    ROUND(
                    (CASE
                        WHEN denominator != 0 THEN numerator / denominator
                        ELSE 0
                    END), 2) AS score
                FROM similarity
        """
        )

        return result

    def get_movie_name(movieID):
        result = spark.sql(
            """
                SELECT movieTitle
                FROM movie_names
                WHERE movieID = {0}
            """.format(
                movieID
            )
        )
        return result.first()["movieTitle"]

    schema_movie_names = StructType(
        [
            StructField("movieID", IntegerType(), False),
            StructField("movieTitle", StringType(), False),
        ]
    )

    schema_movies = StructType(
        [
            StructField("userID", IntegerType(), False),
            StructField("movieID", IntegerType(), False),
            StructField("rating", IntegerType(), False),
            StructField("timestamp", LongType(), False),
        ]
    )

    movie_names = (
        spark.read.format("csv")
        .option("charset", "UTF-8")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("sep", "|")
        .schema(schema_movie_names)
        .load(f"file://{data_dir}/ml-100k/u.item")
    )
    movie_names.cache()
    movie_names.createOrReplaceTempView("movie_names")

    movies = (
        spark.read.format("csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("sep", "\t")
        .schema(schema_movies)
        .load(f"file://{data_dir}/ml-100k/u.data")
    )

    ratings = movies.select("userID", "movieID", "rating")
    ratings.cache()
    ratings.createOrReplaceTempView("ratings")

    movie_pairs = spark.sql(
        """
            SELECT
                ratings1.movieID AS movie1,
                ratings2.movieID AS movie2,
                ratings1.rating AS rating1,
                ratings2.rating AS rating2
            FROM ratings AS ratings1 INNER JOIN ratings AS ratings2
                ON ratings1.userID = ratings2.userID AND ratings1.movieID < ratings2.movieID
        """
    )

    movie_pairs_similarities = cosine_similarities(movie_pairs).cache()

    scoreTreshold = 0.97
    coOccurenceTreshold = 50.0

    results = (
        movie_pairs_similarities.filter(
            (
                (movie_pairs_similarities.movie1 == movieID)
                | (movie_pairs_similarities.movie2 == movieID)
            )
            & (movie_pairs_similarities.score > scoreTreshold)
            & (movie_pairs_similarities.numPairs > coOccurenceTreshold)
        )
        .sort(["score", "numPairs"], ascending=False)
        .take(10)
    )

    print(f"Top {len(results)} similar movies to {get_movie_name(movieID)}")
    header = True
    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        if header:
            print("-" * 70)
            print(f"{'MOVIE':<50}{'SCORE':^10}{'STRENGTH':^10}")
            print("-" * 70)
            header = False
        similarMovieID = result.movie1 if result.movie1 != movieID else result.movie2
        print(
            f"{get_movie_name(similarMovieID):<50}{result.score:^10}{result.numPairs:^10}"
        )


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("-m", "--movie", type=int, default=None)
    args = parser.parse_args()
    if args.movie:
        movieID = int(args.movie)

        load_dotenv()
        data_dir = "/opt/bitnami/spark/data/"
        with SparkConfig("MovieSimilarities") as spark:
            movie_recommendations(spark, data_dir, movieID)
