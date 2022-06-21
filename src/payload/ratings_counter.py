# from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from collections import OrderedDict


def ratings_counter(spark: SparkSession, data_dir):
    # conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
    # sc = SparkContext(conf = conf)
    sc = spark.sparkContext

    lines = sc.textFile(f"file://{data_dir}/ml-100k/u.data")
    ratings = lines.map(lambda x: x.split()[2])
    result = ratings.countByValue()

    sortedResults = OrderedDict(sorted(result.items()))
    for key, value in sortedResults.items():
        print("%s %i" % (key, value))
