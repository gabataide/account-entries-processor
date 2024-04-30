from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def create_spark_session(app_name: str) -> SparkSession:
  "Create a local Spark session with as many worker threads as possible"

  return SparkSession.builder \
    .master('local[*]') \
    .appName(app_name) \
    .getOrCreate()


def create_dataframe_from_csv(spark_session: SparkSession, csv_path: str) -> DataFrame:
  "Create a dataframe from a CSV file"

  return spark_session.read.csv(
    path=csv_path,
    inferSchema=True,
    header=True,
    sep=',',
    encoding='UTF-8'
  )
