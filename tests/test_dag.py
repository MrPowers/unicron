import pytest

from unicron.dag import DAG, DAGValidationError
from .custom_transforms import *


def test_add_node():
    dag = DAG()
    dag.add_node('a')
    assert dag.graph == {'a': set()}


def test_add_edge():
    dag = DAG()
    dag.add_node('a')
    dag.add_node('b')
    dag.add_edge('a', 'b')
    assert dag.graph == {'a': set('b'), 'b': set()}


def test_add_edge_integers():
    dag = DAG()
    dag.add_node(1)
    dag.add_node(2)
    dag.add_edge(1, 2)
    assert dag.graph == {1: {2}, 2: set()}


def test_add_edge_people():
    class Person:
      def __init__(self, first_name):
        self.first_name = first_name
    p1 = Person("paula")
    p2 = Person("drew")
    dag = DAG()
    dag.add_node(p1)
    dag.add_node(p2)
    dag.add_edge(p1, p2)
    assert dag.graph == {p1: {p2}, p2: set()}


def test_add_edge_custom_transform():
    dag = DAG()
    dag.add_node(ct_a)
    dag.add_node(ct_ab)
    dag.add_edge(ct_a, ct_ab)
    assert dag.graph == {ct_a: {ct_ab}, ct_ab: set()}


def test_from_dict():
    dag = DAG()
    dag.from_dict({'a': ['b', 'c'],
                   'b': ['d'],
                   'c': ['d'],
                   'd': []})
    assert dag.graph == {'a': set(['b', 'c']),
                         'b': set('d'),
                         'c': set('d'),
                         'd': set()}


def test_reset_graph():
    dag = DAG()
    dag.add_node('a')
    assert dag.graph == {'a': set()}
    dag.reset_graph()
    assert dag.graph == {}


def test_independent_nodes():
    dag = DAG()
    dag.from_dict({'a': ['b', 'c'],
                   'b': ['d'],
                   'c': ['b'],
                   'd': []})
    assert dag.independent_nodes(dag.graph) == ['a']


def test_topological_sort():
    dag = DAG()
    dag.from_dict({'a': [],
                   'b': ['a'],
                   'c': ['b']})
    assert dag.topological_sort() == ['c', 'b', 'a']


# def test_successful_validation():
    # dag = DAG()
    # assert dag.validate()[0] is True


# def test_failed_validation():
    # dag = DAG()
    # dag.from_dict({'a': ['b'],
                   # 'b': ['a']})


def test_downstream():
    dag = DAG()
    dag.from_dict({'a': ['b', 'c'],
                   'b': ['d'],
                   'c': ['b'],
                   'd': []})
    assert set(dag.downstream('a', dag.graph)) == set(['b', 'c'])


def test_all_downstreams():
    dag = DAG()
    dag.from_dict({'a': ['b', 'c'],
                   'b': ['d'],
                   'c': ['b'],
                   'd': []})
    assert dag.all_downstreams('a') == ['c', 'b', 'd']
    assert dag.all_downstreams('b') == ['d']
    assert dag.all_downstreams('d') == []


def test_all_downstreams_pass_graph():
    dag = DAG()
    dag.from_dict({'a': ['b', 'c'],
                   'b': ['d'],
                   'c': ['b'],
                   'd': []})
    dag2 = DAG()
    dag2.from_dict({'a': ['c'],
                    'b': ['d'],
                    'c': ['d'],
                    'd': []})
    assert dag.all_downstreams('a', dag2.graph) == ['c', 'd']
    assert dag.all_downstreams('b', dag2.graph) == ['d']
    assert dag.all_downstreams('d', dag2.graph) == []


def test_predecessors():
    dag = DAG()
    dag.from_dict({'a': ['b', 'c'],
                   'b': ['d'],
                   'c': ['b'],
                   'd': []})
    assert set(dag.predecessors('a')) == set([])
    assert set(dag.predecessors('b')) == set(['a', 'c'])
    assert set(dag.predecessors('c')) == set(['a'])
    assert set(dag.predecessors('d')) == set(['b'])


def test_all_leaves():
    dag = DAG()
    dag.from_dict({'a': ['b', 'c'],
                   'b': ['d'],
                   'c': ['b'],
                   'd': []})
    assert dag.all_leaves() == ['d']


def test_size():
    dag = DAG()
    dag.from_dict({'a': ['b', 'c'],
                   'b': ['d'],
                   'c': ['b'],
                   'd': []})
    assert dag.size() == 4
    dag.delete_node('a')
    assert dag.size() == 3


def test_shortest_path():
    dag = DAG()
    dag.from_dict({'a': ['b', 'c'],
                   'b': ['d'],
                   'c': ['b'],
                   'd': []})
    assert dag.shortest_path('a', 'd') == ['a', 'b', 'd']


def test_shortest_path_with_custom_transforms():
    dag = DAG()
    dag.from_dict({ct_a: [ct_ab],
                   ct_ab: [ct_abc],
                   ct_abc: []})
    assert dag.shortest_path(ct_a, ct_abc) == [ct_a, ct_ab, ct_abc]

