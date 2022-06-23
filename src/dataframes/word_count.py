# Install Spacy
# pip install -U pip setuptools wheel
# pip install -U spacy
# python -m spacy download en_core_web_sm

import sys
import os
from dotenv import load_dotenv
import pandas as pd
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
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

    def tokenize_and_clean(data):
        spacy_model = get_spacy_model()
        doc = spacy_model.pipe(data)
        tokens = [
            [token.lemma_ for token in sentence if not token.is_stop and token.text]
            for sentence in doc
        ]
        return pd.Series(tokens)

    @pandas_udf("integer", PandasUDFType.SCALAR)
    def pandas_tokenize(x):
        return x.apply(tokenize_and_clean)

    tokenize_pandas = spark.udf.register("tokenize_pandas", pandas_tokenize)

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
        .load(f"file://{data_dir}/book2.txt")
    )

    tokenized = book.select(tokenize_pandas("text"))
    tokenized.show()

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
