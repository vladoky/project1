from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
from utils.processing_functions import normalize_column_names



def spark_session():
    """Fixture for creating a spark context."""
    return SparkSession.builder. \
        appName("pyspark-test"). \
        getOrCreate()


def test_normalize_column_names(df: DataFrame):
    test_data: list = [('Job id', 'Annual', 'Posting type'), ('Agency', 'Daily', 'Posting type'),('Posting type', 'ORA','Posting type')]
    schema: list = ['Job id', 'Agency', "Posting Type"]
    test_df = spark.createDataFrame(data=test_data, schema=schema)
    l = list(normalize_column_names(test_df).columns)
    assert l.pop(2) in ['posting_type']