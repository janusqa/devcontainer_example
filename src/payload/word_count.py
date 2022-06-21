from pyspark.sql import SparkSession

# from pyspark import SparkConf, SparkContext


def word_count(spark: SparkSession, data_dir: str):
    # conf = SparkConf().setMaster("local").setAppName("WordCount")
    # sc = SparkContext(conf = conf)

    sc = spark.sparkContext

    input = sc.textFile(f"file://{data_dir}/book.txt")
    words = input.flatMap(lambda x: x.split())
    word_counts = words.countByValue()

    for word, count in word_counts.items():
        clean_word = word.encode("ascii", "ignore")
        if clean_word:
            print(f"{clean_word.decode()} {count}")
