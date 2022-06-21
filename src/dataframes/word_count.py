import sys
import os
from dotenv import load_dotenv

import pandas as pd

# import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql import functions as func


current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from spark_config import SparkConfig


def word_count(spark: SparkSession, data_dir: str):
    def get_spacy_model():
        global SPACY_MODEL
        if not SPACY_MODEL:
            sm = spacy.load("en_core_web_sm")
        # FIX https://github.com/explosion/spaCy/issues/922
        sm.vocab.add_flag(
            lambda s: s.lower() in spacy.lang.en.stop_words.STOP_WORDS,
            spacy.attrs.IS_STOP,
        )
        SPACY_MODEL = sm
        return SPACY_MODEL

    @pandas_udf(StringType())
    def tokenize_and_clean(text_blob: pd.Series) -> str:
        spacy_model = get_spacy_model()
        doc = spacy_model.pipe(text_blob)
        tokens = [
            [tok.lemma_ for tok in sentence if not tok.is_stop and tok.text]
            for sentence in doc
        ]
        tokens_series = pd.Series(tokens)
        return tokens_series

    schema = StructType(
        [
            StructField("text", StringType(), False),
        ]
    )
    book = (
        spark.read.format("text")
        .option("header", "false")
        .option("inferSchema", "false")
        .schema(schema)
        .load(f"file://{data_dir}/book.txt")
    )
    book.select(tokenize_and_clean(book.text)).show()
    # df = pd.DataFrame()
    # words = book.select(func.explode(func.split(book.text, "\\W+")).alias("word"))
    # words.cache()
    # words.createOrReplaceTempView("words")
    # words_count = spark.sql(
    #     """
    #     SELECT word, COUNT(word) AS word_count
    #     FROM words
    #     WHERE TRIM(word) != ""
    #     GROUP BY word
    #     ORDER BY word_count DESC
    #     """
    # )
    # words_count.show()
    # words.filter(words.word != "")
    # lower_case_words = words.select(func.lower(words.word).alias("word"))
    # word_counts = lower_case_words.groupBy("word").count()
    # word_counts_sorted = word_counts.sort("count")
    # word_counts_sorted.show(word_counts_sorted.count())


if __name__ == "__main__":
    load_dotenv()
    data_dir = "/opt/bitnami/spark/data/"
    with SparkConfig("CustomerOrders") as spark:
        word_count(spark, data_dir)
