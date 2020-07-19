# unicron

PySpark custom transformation runner that manages function order dependencies with a DAG.

See [this video](https://www.youtube.com/watch?v=hzNsOGt3bHk) for more info about Unicron, a "[god of chaos who devours realities](https://en.wikipedia.org/wiki/Unicron)".

This library is a diety of PySpark that helps you run your transformations in the right order.

## Problem unicron solves

Many organizations have a library of transformations that need to be run in a specific order to get the desired result.

Figuring out how to run 30 functions in the right order to append the column you need for your analysis is painful.

unicron lets you specify the column you want appended and will do the hard work of figuring out what transformations need to be run and in what order.

unicron does this by modeling your transformations in a directed acyclic graph data structure.  unicron layers a DAG model on top of the DAG model that Spark uses under the hood when executing code.  DAGs on DAGs!

## Example

Suppose you have the following DataFrame:

```
+-----+
| name|
+-----+
| jose|
|   li|
|luisa|
+-----+
```

A non-technical user would like to append column `d` to the DataFrame.  They don't know what transformation appends column `d` or the list of transformations that might need to be run before the "column d transformation" can be run.  Here's the interface to append column `d`:

```python
actual_df = unicron.add_column(df, graph, "d")
actual_df.show()
```

```
+-----+---+---+---+
| name|  a|  b|  d|
+-----+---+---+---+
| jose|aaa|bbb|ddd|
|   li|aaa|bbb|ddd|
|luisa|aaa|bbb|ddd|
+-----+---+---+---+
```

unicron appended columns `a` and `b` in order to append column `d`.  The work unicron does under the hood to append columns in the right order is hidden from the non-technical user.

Some users will "quit Spark" when they realize that it's too complicated for them to figure out how to append the column they want for their analysis.  This interface makes Spark more approachable.

The technical users should be responsible for constructing the `graph` and passing it to notebook users.

## Implementation

The following directed graph of custom transformations will be used to demonstrate the functionality of this library ([image generated here](https://csacademy.com/app/graph_editor/)).

![graph_example](https://github.com/MrPowers/unicron/blob/master/images/directed_graph.png)

Let's review some directed acyclic graph (DAG) basics:

* root, a, b, c, d, and e are vertices
* the arrows that connect the vertices are called edges
* the shortest path between vertices root and e is root => a => e.  You could also get from root to e via root => a => b => d => e, but that'd be longer.
* graphs can be [topologically sorted](https://en.wikipedia.org/wiki/Topological_sorting).  The topological sorting of this graph is root => a => b => d => e => c

unicron models each vertex in the directed graph as a `CustomTransform` object with these four properties:

* transform: [a function with this signature](https://mungingdata.com/pyspark/chaining-dataframe-transformations/)
* cols_added: columns added by the transform function
* cols_removed: columns removed by the transform function
* required_cols: columns the transform function depends on to run successfully

Here's how we can create a `CustomTransform`:

```python
def with_b():
    def _(df):
        if "a" not in df.columns:
            raise ColumnsDoesNotExistError("no a column")
        return df.withColumn("b", F.lit("bbb"))
    return _


b = CustomTransform(with_b, cols_added = ["b"], required_cols = ["a"])
```

`with_b` requires column `a` and appends column `b` to a DataFrame.

The [networkx](https://github.com/networkx/networkx) library makes it easy to construct a directed graph with `CustomTransform` objects as vertices:

```python
import networkx as nx

graph = nx.DiGraph()
graph.add_edges_from([(root, a), (a, b), (a, e), (b, c), (b, d), (d, e)])
```

**Single root**

DAGs can have multiple roots, but it's best for you to create a single root, even it it does nothing, so the topological sort always starts with the same vertex.  Here's an example of a root that doesn't do anything:

```python
def with_root():
    def _(df):
        return df
    return _


root = CustomTransform(with_root)
```

**DAG validity**

TODO

**CustomTransform Validation**

TODO

**Skipping transformations when columns are present**

TODO

## Public interface

Suppose you have three transformations that append columns to a DataFrame:

* `with_col_a` appends `col_a`
* `with_col_ab` appends `col_ab`
* `with_col_abc` appends `col_abc`

These functions are order dependent so you need to run `with_col_a` first, then `with_col_ab`, then `with_col_abc`.

Suppose an end user wants `col_abc` appended to the DataFrame and they don't want to concern themselves with running all the transformations in a specific order.

unicron lets users specify the columns they want appended to a DataFrame and is responsible for running the required transforations in the correct order.  This is a better user interface for programmers to provide for less technical Spark users.

```python
import unicron

# unicron will intelligently run with_col_a, with_col_ab, and with_col_abc in the right order
# to append col_abc to the DataFrame
new_df = unicron.transform(df, dag, cols = ["col_abc"])

# unicron provides a list of all the columns it can append
unicron.list_cols(dag) # [col_a, col_ab, col_abc]

# unicron can validate that a custom transformation DAG is valid
unicron.validate_dag(dag) # throws an error if the DAG isn't structured logically
```

## todo

* add the PySpark dependency

