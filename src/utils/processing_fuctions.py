import pandas as pd
import numpy as np
from pyspark.sql.functions import isnan, when, count, col, cast, substring, expr
from pyspark.ml.feature import  Tokenizer
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf




def normalize_column_names(df: DataFrame):
    df = df.select([F.col(i).alias(i.lower())for i in df.columns])
    renamed_df = df.select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])
    return renamed_df


def column_types(df: DataFrame):
    numeric_columns = list()
    categorical_column = list()
    timestamp_column = list()
    for col_ in df.columns:
        if df.select(col_).dtypes[0][1] == "string":
            categorical_column.append(col_)
        elif df.select(col_).dtypes[0][1] == "timestamp":
            timestamp_column.append(col_)
        else:
            numeric_columns.append(col_)

    print("Numeric columns",numeric_columns)
    print("\nCategorical columns",categorical_column)
    print("\nTimestamp columns",timestamp_column)
    

