from pathlib import Path

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame

PROJECT_ROOT = Path(__file__).parent.parent.parent.as_posix()

conf = SparkConf().setAppName('pySparkExamples').setMaster('local')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


def print_data_frame(df: DataFrame):
    df.printSchema()
    df.show(truncate=False)