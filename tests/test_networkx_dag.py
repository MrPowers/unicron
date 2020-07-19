import pytest

import networkx as nx

def test_network_x_dag():
    graph = nx.DiGraph()
    graph.add_edges_from(
        [('A', 'B'), ('A', 'C'), ('D', 'B'), ('E', 'C'), ('E', 'F'),
         ('B', 'H'), ('B', 'G'), ('B', 'F'), ('C', 'G')])
    assert nx.shortest_path(graph, 'A', 'G') == ['A', 'B', 'G']


def test_topological_sort():
    graph = nx.DiGraph()
    graph.add_edges_from([("root", "a"), ("a", "b"), ("a", "e"), ("b", "c"), ("b", "d"), ("d", "e")])
    assert list(nx.topological_sort(graph)) == ['root', 'a', 'b', 'd', 'e', 'c']


def test_shortest_path():
    graph = nx.DiGraph()
    graph.add_edges_from([("root", "a"), ("a", "b"), ("a", "e"), ("b", "c"), ("b", "d"), ("d", "e")])
    assert list(nx.shortest_path(graph, "root", "d")) == ['root', 'a', 'b', 'd']

