import sys
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from spark_config import SparkConfig


def friends_by_age(spark: SparkSession, data_dir: str):
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), False),
            StructField("num_friends", IntegerType(), False),
        ]
    )
    fake_friends_df = (
        spark.read.format("csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("sep", ",")
        .schema(schema)
        .load(f"file://{data_dir}/fakefriends.csv")
    )
    fake_friends_df.cache()
    fake_friends_df.createOrReplaceTempView("fakefriends")
    average_friends_per_age = spark.sql(
        """
        SELECT age, CAST(AVG(num_friends) AS DECIMAL(5,2)) AS average_friends
        FROM fakefriends
        GROUP BY age
        ORDER BY age ASC
        """
    )
    average_friends_per_age.show()


if __name__ == "__main__":
    load_dotenv()
    data_dir = "/opt/bitnami/spark/data/"
    with SparkConfig("CustomerOrders") as spark:
        friends_by_age(spark, data_dir)
