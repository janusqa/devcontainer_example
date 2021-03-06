import os
import socket

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


class SparkConfig:
    def __init__(self, app_name: str):
        self.app_name = app_name
        self.conf = SparkConf()
        os.environ["SPARK_LOCAL_IP"] = socket.gethostbyname(
            os.environ.get("SPARK_DRIVER_HOST")
        )

    @property
    def session(self) -> SparkSession:
        self.conf.setAll(
            [
                (
                    "spark.master",
                    os.environ.get("SPARK_MASTER_URL"),
                ),
                (
                    "spark.driver.host",
                    os.environ.get("SPARK_DRIVER_HOST"),
                ),
                (
                    "spark.submit.deployMode",
                    "client",
                ),
                (
                    "spark.driver.bindAddress",
                    "0.0.0.0",
                ),
                (
                    "spark.app.name",
                    self.app_name,
                ),
                (
                    "spark.sql.execution.arrow.pyspark.enabled",
                    "true",
                ),
                (
                    "spark.sql.execution.arrow.pyspark.fallback.enabled",
                    "true",
                ),
                ("spark.sql.execution.arrow.maxRecordsPerBatch", 10000),
                (
                    "spark.pyspark.python",
                    os.environ.get("PYSPARK_PYTHON"),
                ),
                (
                    "spark.pyspark.driver.python",
                    os.environ.get("PYSPARK_DRIVER_PYTHON"),
                ),
                (
                    "spark.archives",
                    "venv.tar.gz#venv",
                ),
            ]
        )
        return SparkSession.builder.config(conf=self.conf).getOrCreate()

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc_val, traceback):
        self.session.stop()
