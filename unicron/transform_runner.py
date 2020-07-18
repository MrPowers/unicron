import networkx as nx


def transform_shortest_path(df, graph, start, end):
    transforms = nx.shortest_path(graph, start, end)
    return run_custom_transforms(df, transforms)


def simulate_shortest_path(df, graph, start, end, skip_when_possible = False):
    transforms = nx.shortest_path(graph, start, end)
    cols_added = []
    cols_removed = []
    df_columns = df.columns
    for ct in transforms:
        if skip_when_possible:
            col_already_present = list(map(lambda col_name: col_name not in df_columns, ct.cols_added))
            if all(col_already_present):
                cols_added = ct.cols_added + cols_added
                cols_removed = ct.cols_removed + cols_removed
        else:
            cols_added = ct.cols_added + cols_added
            cols_removed = ct.cols_removed + cols_removed
    return {"cols_added": cols_added, "cols_removed": cols_removed}


def transforms_to_run(df, graph, start, end, skip_when_possible = False):
    transforms = nx.shortest_path(graph, start, end)
    if skip_when_possible == False:
        return transforms
    else:
        res = []
        df_columns = df.columns
        for ct in transforms:
            col_already_present = list(map(lambda col_name: col_name not in df_columns, ct.cols_added))
            if all(col_already_present):
                res = res ++ ct
        return res


def run_custom_transforms(df, custom_transforms):
    res = df
    for ct in custom_transforms:
        res = res.transform(ct.transform())
    return res
