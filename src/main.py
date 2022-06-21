from dotenv import load_dotenv
from spark_config import SparkConfig
from payload.example_1 import run_spark_example_1
from payload.example_2 import run_spark_example_2
from payload.ratings_counter import ratings_counter
from payload.friends_by_age import friends_by_age
from payload.min_temperatures import min_temperatures_year_1800
from payload.word_count import word_count
from payload.word_count_better import word_count_better
from payload.word_count_better_sorted import word_count_better_sorted
from payload.customer_orders import customer_orders


if __name__ == "__main__":
    load_dotenv()
    data_dir = "/opt/bitnami/spark/data/"
    # spark = SparkConfig("Example1").session
    # run_spark_example_1(spark, data_dir)

    # spark = SparkConfig("Example2").session
    # run_spark_example_2(spark, data_dir)

    # spark = SparkConfig("RatingsHistogram").session
    # ratings_counter(spark, data_dir)

    # spark = SparkConfig("FriendsByAge").session
    # friends_by_age(spark, data_dir)

    # spark = SparkConfig("MinTemperaturesIn1800").session
    # min_temperatures_year_1800(spark, data_dir)

    # spark = SparkConfig("WordCount").session
    # word_count(spark, data_dir)

    # spark = SparkConfig("WordCount").session
    # word_count_better(spark, data_dir)

    # spark = SparkConfig("WordCount").session
    # word_count_better_sorted(spark, data_dir)

    spark = SparkConfig("CustomerOrders").session
    customer_orders(spark, data_dir)

    spark.stop()
