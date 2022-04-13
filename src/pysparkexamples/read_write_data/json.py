import sys

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pysparkexamples import spark, PROJECT_ROOT

JSON_FILE = f"{PROJECT_ROOT}/data/emp_details.json"
URI_SCHEME = "file://"
FILE_SCHEMA = StructType([
    StructField("userId", StringType(), True),
    StructField("jobTitle", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("employeeCode", StringType(), True),
    StructField("region", StringType(), True),
    StructField("phoneNumber", StringType(), True),
    StructField("emailAddress", StringType(), True),
    StructField("salary", IntegerType(), True)
])


def read_json(uri_scheme: str = URI_SCHEME, file_name: str = JSON_FILE) -> DataFrame:
    """
    Function used to read json file
    it will read data/emp_details.json by default

    :param uri_scheme: uri scheme e.g. file, hdfs, gcs, s3
    :param file_name: absolute path of file to read
    :return: DataFrame object
    """
    # NOTE - for reading file from local system use UNIX like path string
    print(f"Reading JSON File: {JSON_FILE}")
    input_path = f"{uri_scheme}/{file_name}" if sys.platform == "win32" else f"{uri_scheme}{file_name}"
    return spark.read.json(input_path)


def write_json(df: DataFrame, output_dir: str) -> None:
    """
    Function used to write a DataFrame to json file

    :param df: Dataframe object
    :param output_dir: relative or absolute path of target file
    :return: None
    """
    print(f"Writing DataFrame to JSON file {output_dir}")
    df.write.json(output_dir, mode="overwrite")
