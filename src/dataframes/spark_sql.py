import sys
import os
from dotenv import load_dotenv
from pyspark.sql import Row
from pyspark.sql import SparkSession

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from spark_config import SparkConfig


# Create a SparkSession


def fake_friends(spark: SparkSession, data_dir: str):
    def mapper(line):
        fields = line.split(",")
        return Row(
            ID=int(fields[0]),
            name=str(fields[1].encode("utf-8")),
            age=int(fields[2]),
            numFriends=int(fields[3]),
        )

    lines = spark.sparkContext.textFile(f"file://{data_dir}/fakefriends.csv")
    people = lines.map(mapper)

    # Infer the schema, and register the DataFrame as a table.
    schemaPeople = spark.createDataFrame(people).cache()
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

    # The results of SQL queries are RDDs and support all the normal RDD operations.
    for teen in teenagers.collect():
        print(teen)

    # We can also use functions instead of SQL queries:
    schemaPeople.groupBy("age").count().orderBy("age").show()


if __name__ == "__main__":
    load_dotenv()
    data_dir = "/opt/bitnami/spark/data/"
    with SparkConfig("CustomerOrders") as spark:
        fake_friends(spark, data_dir)
