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
        # if "root" not in df.columns:
            # raise ColumnsDoesNotExistError("no root column")
        return df.withColumn("a", F.lit("aaa"))
    return _


a = CustomTransform(with_a, cols_added = ["a"])



def with_b():
    def _(df):
        if "a" not in df.columns:
            raise ColumnsDoesNotExistError("no a column")
        return df.withColumn("b", F.lit("bbb"))
    return _


b = CustomTransform(with_b, cols_added = ["b"])



def with_c():
    def _(df):
        if "b" not in df.columns:
            raise ColumnsDoesNotExistError("no b column")
        return df.withColumn("c", F.lit("ccc"))
    return _


c = CustomTransform(with_c, cols_added = ["c"])


def with_d():
    def _(df):
        if "b" not in df.columns:
            raise ColumnsDoesNotExistError("no b column")
        return df.withColumn("d", F.lit("ddd"))
    return _


d = CustomTransform(with_c, cols_added = ["d"])


def with_e():
    def _(df):
        if "a" not in df.columns:
            raise ColumnsDoesNotExistError("no a column")
        return df.withColumn("e", F.lit("eee"))
    return _


e = CustomTransform(with_e, cols_added = ["e"])

dag = unicron.DAG()
dag.from_dict({root: [a],
               a: [b, e],
               b: [c, d],
               d: [e],
               e: []})


def test_root_to_b():
    data = [("jose",), ("li",), ("luisa",)]
    df = spark.createDataFrame(data, ["name"])
    actual_df = unicron.transform_shorted_path(df, dag, root, b)
    actual_df.show()
    # expected_data = [
        # ("jose", "jose", "a", "aba", "abcaba"),
        # ("li", "li", "a", "aba", "abcaba"),
        # ("luisa", "laura", "a", "aba", "abcaba")]
    # expected_df = spark.createDataFrame(expected_data, ["name", "expected_name", "col_a", "col_ab", "col_abc"])
    # chispa.assert_df_equality(actual_df, expected_df, ignore_nullable = True)



# def test_root_to_e():
    # data = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
    # df = spark.createDataFrame(data, ["name", "expected_name"])
    # dag = unicron.DAG()
    # dag.from_dict({ct_a: [ct_ab],
                   # ct_ab: [ct_abc],
                   # ct_abc: []})
    # actual_df = unicron.transform_from_root(df, dag, ct_abc)
    # expected_data = [
        # ("jose", "jose", "a", "aba", "abcaba"),
        # ("li", "li", "a", "aba", "abcaba"),
        # ("luisa", "laura", "a", "aba", "abcaba")]
    # expected_df = spark.createDataFrame(expected_data, ["name", "expected_name", "col_a", "col_ab", "col_abc"])
    # chispa.assert_df_equality(actual_df, expected_df, ignore_nullable = True)

