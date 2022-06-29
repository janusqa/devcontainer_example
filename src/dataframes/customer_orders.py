import sys
import os
from dotenv import load_dotenv
import pandas as pd
from pyspark.sql import SparkSession, Column as SparkColumn
from pyspark.sql.functions import pandas_udf, explode, udf
from pyspark.sql.types import (
    StringType,
    FloatType,
    IntegerType,
    StructField,
    StructType,
)


current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from spark_config import SparkConfig


def customer_orders(spark: SparkSession, data_dir: str):

    schema = StructType(
        [
            StructField("customerID", StringType(), False),
            StructField("orderID", StringType(), False),
            StructField("spend", FloatType(), False),
        ]
    )

    cus_data = (
        spark.read.format("csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .schema(schema)
        .load(f"file://{data_dir}/customer-orders.csv")
    )

    cus_data.cache()
    cus_data.createOrReplaceTempView("cus_data")
    cus_order_summary = spark.sql(
        """
        SELECT customerID, ROUND(SUM(spend), 2) as total_spend
        FROM cus_data
        GROUP BY customerID
        ORDER BY total_spend DESC;
        """
    )
    cus_order_summary.show(cus_order_summary.count())


if __name__ == "__main__":
    load_dotenv()
    data_dir = "/opt/bitnami/spark/data/"
    with SparkConfig("CustomerOrders") as spark:
        customer_orders(spark, data_dir)
