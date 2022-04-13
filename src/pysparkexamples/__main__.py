from datetime import date

from pyspark.sql import Row

from pysparkexamples import spark, print_data_frame, PROJECT_ROOT
from pysparkexamples.read_write_data.csv import read_csv, write_csv
from pysparkexamples.read_write_data.json import read_json, write_json
from pysparkexamples.read_write_data.mysql import read_table, write_table


def test():
    df = spark.createDataFrame([
        Row(a=1, b=2., c='string1', d=date(2000, 1, 1)),
        Row(a=2, b=3., c='string2', d=date(2000, 2, 1)),
        Row(a=4, b=5., c='string3', d=date(2000, 3, 1))
    ])
    df.show()


def main():
    emp_details_file_name = f"{PROJECT_ROOT}/data/emp_details.csv"
    # Read CSV File
    # df = read_csv()

    # Read JSON File
    df = read_json()

    # Read table from MySQL
    # df = read_table()

    print_data_frame(df)

    # Write to CSV
    # output_dir = f"{PROJECT_ROOT}/output/diabetes"
    # write_csv(df, output_dir)

    # Write to JSON File
    # output_dir = f"{PROJECT_ROOT}/output/emp_details"
    # write_json(df, output_dir)

    # Write to MySQL Table
    write_table(df, table_name="emp_details")


if __name__ == "__main__":
    main()
