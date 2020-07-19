import pytest

import pyspark.sql.functions as F
import unicron
import chispa

from pyspark.sql import SparkSession
from unicron.custom_transform import CustomTransform

import networkx as nx


spark = SparkSession.builder \
  .master("local") \
  .appName("unicron") \
  .getOrCreate()


class ColumnsDoesNotExistError(Exception):
   """The column does not exist"""
   pass


def with_root():
    def _(df):
        return df
    return _


root = CustomTransform(with_root)



def with_a():
    def _(df):
        return df.withColumn("a", F.lit("aaa"))
    return _


a = CustomTransform(with_a, cols_added = ["a"])



def with_b():
    def _(df):
        if "a" not in df.columns:
            raise ColumnsDoesNotExistError("no a column")
        return df.withColumn("b", F.lit("bbb"))
    return _


b = CustomTransform(with_b, cols_added = ["b"], required_cols = ["a"])



def with_c():
    def _(df):
        if "b" not in df.columns:
            raise ColumnsDoesNotExistError("no b column")
        return df.withColumn("c", F.lit("ccc"))
    return _


c = CustomTransform(with_c, cols_added = ["c"], required_cols = ["b"])


def with_d():
    def _(df):
        if "b" not in df.columns:
            raise ColumnsDoesNotExistError("no b column")
        return df.withColumn("d", F.lit("ddd"))
    return _


d = CustomTransform(with_d, cols_added = ["d"], required_cols = ["b"])


def with_e():
    def _(df):
        if "a" not in df.columns:
            raise ColumnsDoesNotExistError("no a column")
        return df.withColumn("e", F.lit("eee"))
    return _


e = CustomTransform(with_e, cols_added = ["e"], required_cols = ["a"])


graph = nx.DiGraph()
graph.add_edges_from([(root, a), (a, b), (a, e), (b, c), (b, d), (d, e)])


def test_root_to_b():
    data = [("jose",), ("li",), ("luisa",)]
    df = spark.createDataFrame(data, ["name"])
    transforms = unicron.transforms_to_run(df, graph, root, b)
    actual_df = unicron.run_custom_transforms(df, transforms)
    expected_data = [
        ("jose", "aaa", "bbb"),
        ("li", "aaa", "bbb"),
        ("luisa", "aaa", "bbb")]
    expected_df = spark.createDataFrame(expected_data, ["name", "a", "b"])
    chispa.assert_df_equality(actual_df, expected_df, ignore_nullable = True)


def test_root_to_e():
    data = [("jose",), ("li",), ("luisa",)]
    df = spark.createDataFrame(data, ["name",])
    transforms = unicron.transforms_to_run(df, graph, root, e)
    actual_df = unicron.run_custom_transforms(df, transforms)
    expected_data = [
        ("jose", "aaa", "eee"),
        ("li", "aaa", "eee"),
        ("luisa", "aaa", "eee")]
    expected_df = spark.createDataFrame(expected_data, ["name", "a", "e"])
    chispa.assert_df_equality(actual_df, expected_df, ignore_nullable = True)


def test_root_to_d():
    data = [("jose",), ("li",), ("luisa",)]
    df = spark.createDataFrame(data, ["name",])
    transforms = unicron.transforms_to_run(df, graph, root, d)
    actual_df = unicron.run_custom_transforms(df, transforms)
    expected_data = [
        ("jose", "aaa", "bbb", "ddd"),
        ("li", "aaa", "bbb", "ddd"),
        ("luisa", "aaa", "bbb", "ddd")]
    expected_df = spark.createDataFrame(expected_data, ["name", "a", "b", "d"])
    chispa.assert_df_equality(actual_df, expected_df, ignore_nullable = True)


def test_add_column_d():
    data = [("jose",), ("li",), ("luisa",)]
    df = spark.createDataFrame(data, ["name",])
    actual_df = unicron.add_column(df, graph, "d")
    expected_data = [
        ("jose", "aaa", "bbb", "ddd"),
        ("li", "aaa", "bbb", "ddd"),
        ("luisa", "aaa", "bbb", "ddd")]
    expected_df = spark.createDataFrame(expected_data, ["name", "a", "b", "d"])
    chispa.assert_df_equality(actual_df, expected_df, ignore_nullable = True)

