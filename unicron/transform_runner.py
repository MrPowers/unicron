def run_custom_transforms(df, custom_transforms):
    res = df
    for ct in custom_transforms:
        res = res.transform(ct.transform())
    return res
