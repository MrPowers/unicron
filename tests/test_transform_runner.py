import pytest

import pyspark.sql.functions as F
import unicron
import chispa

from .custom_transforms import *
from unicron.custom_transform import CustomTransform

from pyspark.sql import SparkSession

import networkx as nx

spark = SparkSession.builder \
  .master("local") \
  .appName("unicron") \
  .getOrCreate()


def test_transform_shortest_path():
    data = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
    df = spark.createDataFrame(data, ["name", "expected_name"])
    graph = nx.DiGraph()
    graph.add_edges_from([(ct_a, ct_ab), (ct_ab, ct_abc)])
    actual_df = unicron.transform_shortest_path(df, graph, ct_a, ct_abc)
    expected_data = [
        ("jose", "jose", "a", "aba", "abcaba"),
        ("li", "li", "a", "aba", "abcaba"),
        ("luisa", "laura", "a", "aba", "abcaba")]
    expected_df = spark.createDataFrame(expected_data, ["name", "expected_name", "col_a", "col_ab", "col_abc"])
    chispa.assert_df_equality(actual_df, expected_df, ignore_nullable = True)


def test_simulate_shortest_path():
    data = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
    df = spark.createDataFrame(data, ["name", "expected_name"])
    graph = nx.DiGraph()
    graph.add_edges_from([(ct_a, ct_ab), (ct_ab, ct_abc)])
    actual = unicron.simulate_shortest_path(df, graph, ct_a, ct_abc)
    expected = {"cols_added": ["col_abc", "col_ab", "col_a"], "cols_removed": []}
    assert actual == expected


def test_simulate_shortest_path_with_skip():
    data = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
    df = spark.createDataFrame(data, ["name", "col_ab"])
    graph = nx.DiGraph()
    graph.add_edges_from([(ct_a, ct_ab), (ct_ab, ct_abc)])
    actual = unicron.simulate_shortest_path(df, graph, ct_a, ct_abc, True)
    expected = {"cols_added": ["col_abc", "col_a"], "cols_removed": []}
    assert actual == expected


def test_transforms_to_run():
    data = [("jose",), ("li",), ("luisa",)]
    df = spark.createDataFrame(data, ["name"])
    graph = nx.DiGraph()
    graph.add_edges_from([(ct_a, ct_ab), (ct_ab, ct_abc)])
    actual = unicron.transforms_to_run(df, graph, ct_a, ct_abc, False)
    expected = [ct_a, ct_ab, ct_abc]
    assert actual == expected


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


