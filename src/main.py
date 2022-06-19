from dotenv import load_dotenv
from spark_config import SparkConfig
from payload.example_1 import run_spark_example_1
from payload.example_2 import run_spark_example_2
from payload.ratings_counter import ratings_counter


if __name__ == "__main__":
    load_dotenv()
    spark = SparkConfig("RatingsHistogram").session
    # run_spark_example_1(spark)
    # run_spark_example_2(spark)
    ratings_counter(spark)
    spark.stop()
