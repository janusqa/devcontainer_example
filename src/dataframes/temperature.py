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


def min_temp_year_1800(spark: SparkSession, data_dir: str):
    @pandas_udf("float")
    def c_to_f(temp: pd.Series) -> pd.Series:
        return (temp * 0.1 * (9.0 / 5.0) + 32.0).round(decimals=2)

    schema = StructType(
        [
            StructField("stationID", StringType(), False),
            StructField("date", IntegerType(), False),
            StructField("measure_type", StringType(), False),
            StructField("temperature", FloatType(), False),
        ]
    )

    temp_data = (
        spark.read.format("csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .schema(schema)
        .load(f"file://{data_dir}/1800.csv")
    )

    temp_data.cache()
    temp_data.createOrReplaceTempView("temp_data")
    min_temp_per_station = spark.sql(
        """
        SELECT stationID, MIN(temperature) as min_temp
        FROM temp_data
        WHERE measure_type = "TMIN"
        GROUP BY stationID
        ORDER BY min_temp DESC;
        """
    )

    # min_temp_per_station.show(min_temp_per_station.count())
    min_temp_per_station.withColumn("min_temp_f", c_to_f("min_temp")).show()


if __name__ == "__main__":
    load_dotenv()
    data_dir = "/opt/bitnami/spark/data/"
    with SparkConfig("MinTemperature") as spark:
        min_temp_year_1800(spark, data_dir)
