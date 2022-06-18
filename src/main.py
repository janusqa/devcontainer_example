from dotenv import load_dotenv
from spark_config import SparkConfig
from payload.example_1 import run_spark_example_1
from payload.example_2 import run_spark_example_2


if __name__ == "__main__":
    load_dotenv()
    spark = SparkConfig("employees").context
    run_spark_example_2(spark)
    spark.stop()
