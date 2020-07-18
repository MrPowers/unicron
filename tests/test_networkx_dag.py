import pytest

import networkx as nx

def test_network_x_dag():
    graph = nx.DiGraph()
    graph.add_edges_from(
        [('A', 'B'), ('A', 'C'), ('D', 'B'), ('E', 'C'), ('E', 'F'),
         ('B', 'H'), ('B', 'G'), ('B', 'F'), ('C', 'G')])
    assert nx.shortest_path(graph, 'A', 'G') == ['A', 'B', 'G']
