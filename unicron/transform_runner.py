def transform(df, dag, start, end):
    transforms = dag.shortest_path(start, end)
    return run_custom_transforms(df, transforms)


def run_custom_transforms(df, custom_transforms):
    res = df
    for ct in custom_transforms:
        res = res.transform(ct.transform())
    return res
