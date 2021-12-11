from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('pySparkExamples').setMaster('local')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
