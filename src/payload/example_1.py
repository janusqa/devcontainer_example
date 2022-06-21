from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


def run_spark_example_1(spark: SparkSession, data_dir: str) -> None:

    rows = [
        [1, 100],
        [2, 200],
        [3, 300],
    ]

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("salary", IntegerType(), True),
        ]
    )

    df = spark.createDataFrame(rows, schema=schema)

    highest_salary = df.agg({"salary": "max"}).collect()[0]["max(salary)"]

    second_highest_salary = (
        df.filter(f"`salary` < {highest_salary}")
        .orderBy("salary", ascending=False)
        .select("salary")
        .limit(1)
    )

    second_highest_salary.show()
