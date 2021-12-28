import sys

from pyspark.sql import DataFrame

from pysparkexamples import spark, PROJECT_ROOT

CSV_FILE = f"{PROJECT_ROOT}/data/diabetes.csv"
URI_SCHEME = "file://"


def read_csv(uri_scheme: str = URI_SCHEME, file_name: str = CSV_FILE) -> DataFrame:
    """
    Function used to read csv file
    it will read data/diabetes.csv by default

    :param uri_scheme: uri scheme e.g. file, hdfs, gcs, s3
    :param file_name: absolute path of file to read
    :return: DataFrame object
    """
    # NOTE - for reading file from local system use UNIX like path string
    print(f"Reading CSV File: {CSV_FILE}")
    input_path = f"{uri_scheme}/{file_name}" if sys.platform == "win32" else f"{uri_scheme}{file_name}"
    return spark.read.csv(input_path, header=True, inferSchema=True)


def write_csv(df: DataFrame, output_dir: str, is_single_file_enabled: bool = True) -> None:
    """
    Function used to write a DataFrame to csv file

    :param df: Dataframe object
    :param output_dir: relative or absolute path of target file
    :param is_single_file_enabled: if enabled the data will be repartitioned to ensure only 1 target file is created
    :return: None
    """
    print(f"Writing DataFrame to CSV file {output_dir}")
    if is_single_file_enabled:
        df = df.repartition(1)
    df.write.csv(output_dir, header=True, mode="overwrite", )
