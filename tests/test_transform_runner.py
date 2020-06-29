import pytest

import pyspark.sql.functions as F
import unicron
import chispa

from .custom_transforms import *
from unicron.custom_transform import CustomTransform

from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .master("local") \
  .appName("unicron") \
  .getOrCreate()


def test_transform_from_root():
    data = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
    df = spark.createDataFrame(data, ["name", "expected_name"])
    dag = unicron.DAG()
    dag.from_dict({ct_a: [ct_ab],
                   ct_ab: [ct_abc],
                   ct_abc: []})
    actual_df = unicron.transform_from_root(df, dag, ct_abc)
    expected_data = [
        ("jose", "jose", "a", "aba", "abcaba"),
        ("li", "li", "a", "aba", "abcaba"),
        ("luisa", "laura", "a", "aba", "abcaba")]
    expected_df = spark.createDataFrame(expected_data, ["name", "expected_name", "col_a", "col_ab", "col_abc"])
    chispa.assert_df_equality(actual_df, expected_df, ignore_nullable = True)


def test_transform_shorted_path():
    data = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
    df = spark.createDataFrame(data, ["name", "expected_name"])
    dag = unicron.DAG()
    dag.from_dict({ct_a: [ct_ab],
                   ct_ab: [ct_abc],
                   ct_abc: []})
    actual_df = unicron.transform_shorted_path(df, dag, ct_a, ct_abc)
    expected_data = [
        ("jose", "jose", "a", "aba", "abcaba"),
        ("li", "li", "a", "aba", "abcaba"),
        ("luisa", "laura", "a", "aba", "abcaba")]
    expected_df = spark.createDataFrame(expected_data, ["name", "expected_name", "col_a", "col_ab", "col_abc"])
    chispa.assert_df_equality(actual_df, expected_df, ignore_nullable = True)


def test_run_custom_transforms():
    data = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
    df = spark.createDataFrame(data, ["name", "expected_name"])
    transforms = [ct_a, ct_ab, ct_abc]
    actual_df = unicron.run_custom_transforms(df, transforms)
    expected_data = [
        ("jose", "jose", "a", "aba", "abcaba"),
        ("li", "li", "a", "aba", "abcaba"),
        ("luisa", "laura", "a", "aba", "abcaba")]
    expected_df = spark.createDataFrame(expected_data, ["name", "expected_name", "col_a", "col_ab", "col_abc"])
    chispa.assert_df_equality(actual_df, expected_df, ignore_nullable = True)


