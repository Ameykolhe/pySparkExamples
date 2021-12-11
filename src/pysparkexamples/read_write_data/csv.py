from pysparkexamples import spark

from pyspark.sql import DataFrame

CSV_FILE = "data/diabetes.csv"


def read_csv(file_name=CSV_FILE):
    df = spark.read.csv(file_name, header=True, inferSchema=True)
    print(f"File - {file_name}")
    df.printSchema()
    df.show(truncate=False)


def write_csv(data_frame: DataFrame):
    pass
