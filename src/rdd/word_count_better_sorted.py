import re
from pyspark.sql import SparkSession

# from pyspark import SparkConf, SparkContext


def word_count_better_sorted(spark: SparkSession, data_dir: str):
    # conf = SparkConf().setMaster("local").setAppName("WordCount")
    # sc = SparkContext(conf = conf)

    def normalize_words(text):
        return re.compile(r"\W+", re.UNICODE).split(text.lower())

    sc = spark.sparkContext

    input = sc.textFile(f"file://{data_dir}/book.txt")
    words = input.flatMap(normalize_words)
    word_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    word_counts_sorted = word_counts.map(lambda x: (x[1], x[0])).sortByKey()
    results = word_counts_sorted.collect()

    for result in results:
        count = str(result[0])
        word = result[1].encode("ascii", "ignore")
        if word:
            print(f"{word.decode()}:\t\t{count}")
