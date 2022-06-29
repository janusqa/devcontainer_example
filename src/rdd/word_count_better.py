import re
from pyspark.sql import SparkSession

# from pyspark import SparkConf, SparkContext


def word_count_better(spark: SparkSession, data_dir: str):
    # conf = SparkConf().setMaster("local").setAppName("WordCount")
    # sc = SparkContext(conf = conf)

    def normalize_words(text):
        return re.compile(r"\W+", re.UNICODE).split(text.lower())

    sc = spark.sparkContext

    input = sc.textFile(f"file://{data_dir}/book.txt")
    words = input.flatMap(normalize_words)
    word_counts = words.countByValue()

    for word, count in word_counts.items():
        clean_word = word.encode("ascii", "ignore")
        if clean_word:
            print(f"{clean_word.decode()} {count}")
