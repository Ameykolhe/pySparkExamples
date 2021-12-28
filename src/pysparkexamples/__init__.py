from pathlib import Path

from dotenv import load_dotenv
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame

load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent.parent.as_posix()

conf = SparkConf() \
    .setAppName('pySparkExamples') \
    .setMaster('local') \
    .set("spark.jars", "mysql-connector-java-8.0.27.jar")

sc = SparkContext(conf=conf)
spark = SparkSession(sc)


def print_data_frame(df: DataFrame):
    df.printSchema()
    df.show(truncate=False)
