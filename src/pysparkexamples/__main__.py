from datetime import date

from pyspark.sql import Row

from pysparkexamples import spark
from pysparkexamples.read_write_data.csv import read_csv


def main():
    df = spark.createDataFrame([
        Row(a=1, b=2., c='string1', d=date(2000, 1, 1)),
        Row(a=2, b=3., c='string2', d=date(2000, 2, 1)),
        Row(a=4, b=5., c='string3', d=date(2000, 3, 1))
    ])
    df.show()


if __name__ == "__main__":
    read_csv()
