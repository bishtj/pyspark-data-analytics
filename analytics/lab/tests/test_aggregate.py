import pytest
import analytics.testing

from chispa.dataframe_comparer import assert_df_equality

from analytics.testing.conf_test import spark_mock_df_aggregate
from analytics.testing.conf_test import spark
from analytics.lab.src.aggregate import aggregate
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
import string


def order(df: DataFrame, orderCols: list):
    return df.orderBy(orderCols, ascending=False)


@pytest.mark.spark
def test_transform(spark: SparkSession, spark_mock_df_aggregate):
    # ARRANGE
    df, expect = spark_mock_df_aggregate

    # ACT
    actual = aggregate(df)

    order_cols = ['Prefix', 'Product', 'IssYear']

    # ASSERT
    assert_df_equality(order(actual, order_cols), order(expect, order_cols))
