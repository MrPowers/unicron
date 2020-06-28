import pytest

import pyspark.sql.functions as F
import unicron
import chispa

from pyspark.sql import SparkSession
from unicron.custom_transform import CustomTransform


spark = SparkSession.builder \
  .master("local") \
  .appName("unicron") \
  .getOrCreate()


def with_col_a():
    def _(df):
        return df.withColumn("col_a", F.lit("a_hi"))
    return _


ct_a = CustomTransform(with_col_a, cols_added = ["col_a"])


def test_run_transform():
    data = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
    df = spark.createDataFrame(data, ["name", "expected_name"])
    actual_df = df.transform(ct_a.transform())
    expected_data = [("jose", "jose", "a_hi"), ("li", "li", "a_hi"), ("luisa", "laura", "a_hi")]
    expected_df = spark.createDataFrame(expected_data, ["name", "expected_name", "col_a"])
    chispa.assert_df_equality(actual_df, expected_df, ignore_nullable = True)
