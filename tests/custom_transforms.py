import pyspark.sql.functions as F
from unicron.custom_transform import CustomTransform


def with_col_a():
    def _(df):
        return df.withColumn("col_a", F.lit("a"))
    return _


ct_a = CustomTransform(with_col_a, cols_added = ["col_a"])


def with_col_ab():
    def _(df):
        return df.withColumn("col_ab", F.concat(F.lit("ab"), F.col("col_a")))
    return _


ct_ab = CustomTransform(with_col_ab, cols_added = ["col_ab"])


def with_col_abc():
    def _(df):
        return df.withColumn("col_abc", F.concat(F.lit("abc"), F.col("col_ab")))
    return _


ct_abc = CustomTransform(with_col_abc, cols_added = ["col_abc"])


