# Install Spacy
# pip install -U pip setuptools wheel
# pip install -U spacy
# python -m spacy download en_core_web_sm

import sys
import os
from dotenv import load_dotenv
from typing import Iterator
import pandas as pd
import spacy
from pyspark.sql import SparkSession, Column as SparkColumn
from pyspark.sql.functions import pandas_udf, explode, udf
from pyspark.sql.types import StringType, StructField, StructType


current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from spark_config import SparkConfig


def word_count(spark: SparkSession, data_dir: str):
    SPACY_MODEL = None

    def get_spacy_model():
        nonlocal SPACY_MODEL
        if not SPACY_MODEL:
            nlp = spacy.load("en_core_web_sm")
            # FIX https://github.com/explosion/spaCy/issues/922
            nlp.vocab.add_flag(
                lambda s: s.lower() in spacy.lang.en.stop_words.STOP_WORDS,
                spacy.attrs.IS_STOP,
            )
            SPACY_MODEL = nlp
        return SPACY_MODEL

    @pandas_udf("array<string>")
    def tokenize_and_clean(doc: pd.Series) -> pd.Series:
        spacy_model = get_spacy_model()
        doc_lines = list(spacy_model.pipe(doc))
        tokens = [
            [
                token.text.lower()
                for token in line
                if not token.is_stop
                and not token.is_punct
                and token.text
                and len(token.text.strip()) > 0
            ]
            for line in doc_lines
        ]
        return pd.Series(tokens)

    @udf("string")
    def preprocess(book: SparkColumn) -> SparkColumn:
        return bytes(book, "utf-8").decode("utf-8", "ignore")

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

    book = book.withColumn("text", preprocess(book.text))
    word_frequency = (
        book.select(explode(tokenize_and_clean(book.text)).alias("tokens"))
        .groupBy("tokens")
        .count()
        .sort("count", ascending=True)
    )
    word_frequency.show(word_frequency.count())

    # words = book.select(explode(func.split(book.text, "\\W+")).alias("word"))
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
    with SparkConfig("WordCount") as spark:
        word_count(spark, data_dir)
