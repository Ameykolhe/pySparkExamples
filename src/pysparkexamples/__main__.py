from datetime import date

from pyspark.sql import Row

from pysparkexamples import spark, print_data_frame, PROJECT_ROOT
from pysparkexamples.read_write_data.csv import read_csv, write_csv


def test():
    df = spark.createDataFrame([
        Row(a=1, b=2., c='string1', d=date(2000, 1, 1)),
        Row(a=2, b=3., c='string2', d=date(2000, 2, 1)),
        Row(a=4, b=5., c='string3', d=date(2000, 3, 1))
    ])
    df.show()


def main():
    df = read_csv()
    print_data_frame(df)
    output_dir = f"{PROJECT_ROOT}/output/diabetes"
    write_csv(df, output_dir, is_single_file_enabled=False)


if __name__ == "__main__":
    main()
