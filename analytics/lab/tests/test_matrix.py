import pytest
import analytics.testing

from chispa.dataframe_comparer import assert_df_equality

from analytics.testing.conf_test import spark_mock_df_matrix
from analytics.testing.conf_test import spark
from analytics.lab.src.matrix import matrix
from pyspark.sql import DataFrame, SparkSession
import string


def order(df: DataFrame, orderCol: string):
    return df.orderBy([orderCol], ascending=False)


@pytest.mark.spark
def test_matrix_happy_scenario(spark: SparkSession, spark_mock_df_matrix):
    # ARRANGE
    df, expect = spark_mock_df_matrix

    # ACT
    actual = matrix(df)

    order_col = "Product"

    # ASSERT
    assert_df_equality(order(actual, order_col), order(expect, order_col))
