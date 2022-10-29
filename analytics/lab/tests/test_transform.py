import pytest
import analytics.testing

from chispa.dataframe_comparer import assert_df_equality

from analytics.testing.conf_test import spark_mock_df_transform
from analytics.testing.conf_test import spark
from analytics.lab.src.transform import transform
from pyspark.sql import DataFrame, SparkSession
import string


def order(df: DataFrame, orderCol: string):
    return df.orderBy([orderCol], ascending=False)


@pytest.mark.spark
def test_transform(spark: SparkSession, spark_mock_df_transform):
    # ARRANGE
    df, expect = spark_mock_df_transform

    # ACT
    actual = transform(df)

    cols = expect.columns
    order_col = "InsDt"

    # ASSERT
    assert_df_equality(order(actual.select(cols), order_col), order(expect, order_col))
