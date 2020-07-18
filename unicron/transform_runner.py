import networkx as nx


def transform_shortest_path(df, graph, start, end):
    transforms = nx.shortest_path(graph, start, end)
    return run_custom_transforms(df, transforms)


def run_custom_transforms(df, custom_transforms):
    res = df
    for ct in custom_transforms:
        res = res.transform(ct.transform())
    return res
