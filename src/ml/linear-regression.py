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
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors


current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from spark_config import SparkConfig


def linear_regression(spark: SparkSession, data_dir: str):
    # load data and convert to format expected by mllib
    input_lines = spark.sparkContext.textFile(f"file://{data_dir}regression.txt")
    data = input_lines.map(lambda x: x.split(",")).map(
        lambda x: (float(x[0]), Vectors.dense(float(x[1])))
    )

    # convert data RDD to Dataframe
    df = data.toDF(["label", "features"])

    # split dataset into traing and test data
    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # create linear regression model
    lir = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # train model
    model = lir.fit(trainingDF)

    # Generate predictions using or linear regression model for the features in our dataframe
    full_predictions = model.transform(testDF).cache()

    # extract the predictions and known correct labels
    predictions = full_predictions.select("prediction").rdd.map(lambda x: x[0])
    labels = full_predictions.select("label").rdd.map(lambda x: x[0])

    # combine them together using zip
    prediction_and_label = predictions.zip(labels).collect()

    # print the predicted and actual values of each point
    for prediction in prediction_and_label:
        print(prediction)


if __name__ == "__main__":
    # from argparse import ArgumentParser

    # parser = ArgumentParser()
    # parser.add_argument("-u", "--user", type=int, default=None)
    # args = parser.parse_args()
    # if args.user:
    #     userID = int(args.user)

    load_dotenv()
    data_dir = "/opt/bitnami/spark/data/"
    with SparkConfig("LinearRegressionML") as spark:
        linear_regression(spark, data_dir)
