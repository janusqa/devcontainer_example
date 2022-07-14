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
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler


current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from spark_config import SparkConfig


def real_estate_predictions(spark: SparkSession, data_dir: str):
    # load data
    real_estate_data = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",")
        .load(f"file://{data_dir}/realestate.csv")
    )

    assembler = (
        VectorAssembler()
        .setInputCols(["HouseAge", "DistanceToMRT", "NumberConvenienceStores"])
        .setOutputCol("features")
    )

    df = assembler.transform(real_estate_data).select("PriceOfUnitArea", "features")

    # split dataset into traing and test data
    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # create model
    dtr = (
        DecisionTreeRegressor()
        .setFeaturesCol("features")
        .setLabelCol("PriceOfUnitArea")
    )

    # train model
    model = dtr.fit(trainingDF)

    # Generate predictions using or linear regression model for the features in our dataframe
    full_predictions = model.transform(testDF).cache()

    # extract the predictions and known correct labels
    predictions = full_predictions.select("prediction").rdd.map(lambda x: x[0])
    labels = full_predictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])

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
    with SparkConfig("RealEstateDecisionTreeML") as spark:
        real_estate_predictions(spark, data_dir)
