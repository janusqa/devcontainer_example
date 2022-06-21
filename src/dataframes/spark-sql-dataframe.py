import sys
import os
from dotenv import load_dotenv
from pyspark.sql import Row
from pyspark.sql import SparkSession

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from spark_config import SparkConfig


def fake_friends(spark: SparkSession, data_dir: str):
    people = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(f"file://{data_dir}/fakefriends-header.csv")
    )

    print("Here is our inferred schema:")
    people.printSchema()

    print("Let's display the name column:")
    people.select("name").show()

    print("Filter out anyone over 21:")
    people.filter(people.age < 21).show()

    print("Group by age")
    people.groupBy("age").count().show()

    print("Make everyone 10 years older:")
    people.select(people.name, people.age + 10).show()


if __name__ == "__main__":
    load_dotenv()
    data_dir = "/opt/bitnami/spark/data/"
    with SparkConfig("CustomerOrders") as spark:
        fake_friends(spark, data_dir)
