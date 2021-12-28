import os

from pyspark.sql import DataFrame

from pysparkexamples import spark

JDBC_URL = "jdbc:mysql://localhost:3306"
TABLE_NAME = f"{os.environ.get('MYSQL_DB')}.diabetes"


def read_table(jdbc_url: str = JDBC_URL, table_name: str = TABLE_NAME, properties: dict = None) -> DataFrame:
    """
    Function used to read csv file
    it will read data/diabetes.csv by default

    :param jdbc_url: a JDBC URL of the form "jdbc:subprotocol:subname"
    :param table_name: the name of the table
    :param properties: user -> DB username, password -> DB password, driver -> MySQL Driver Class
    :return: DataFrame object
    """
    print(f"Reading Table File: {table_name}")
    if properties is None:
        properties = {
            "user": os.environ.get("MYSQL_USER"),
            "password": os.environ.get("MYSQL_PASSWORD"),
            "driver": "com.mysql.cj.jdbc.Driver"
        }
    return spark.read.jdbc(jdbc_url, table_name, properties=properties)


def write_table(df: DataFrame, jdbc_url: str = JDBC_URL, table_name: str = TABLE_NAME, properties: dict = None) -> None:
    """
    Function used to write a DataFrame to csv file

    :param df: Dataframe object
    :param jdbc_url: a JDBC URL of the form "jdbc:subprotocol:subname"
    :param table_name: the name of the table
    :param properties: user -> DB username, password -> DB password, driver -> MySQL Driver Class
    :return: None
    """
    print(f"Writing DataFrame to table - {table_name}")
    if properties is None:
        properties = {
            "user": os.environ.get("MYSQL_USER"),
            "password": os.environ.get("MYSQL_PASSWORD"),
            "driver": "com.mysql.cj.jdbc.Driver"
        }
    df.write.jdbc(jdbc_url, table_name, properties=properties)
