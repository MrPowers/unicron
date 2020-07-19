import networkx as nx


def simulate_transforms(transforms):
    cols_added = []
    cols_removed = []
    for ct in transforms:
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
            col_already_present = list(map(lambda col_name: col_name in df_columns, ct.cols_added))
            if not any(col_already_present):
                res.append(ct)
        return res


def run_custom_transforms(df, transforms):
    res = df
    for ct in transforms:
        res = res.transform(ct.transform())
    return res


def add_column(df, graph, col_name):
    transforms = list(nx.topological_sort(graph))
    root = transforms[0]
    end = next(ct for ct in transforms if col_name in ct.cols_added)
    t = transforms_to_run(df, graph, root, end)
    return run_custom_transforms(df, t)

