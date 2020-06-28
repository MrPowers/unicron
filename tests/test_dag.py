import pytest

from unicron.dag import DAG, DAGValidationError


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
