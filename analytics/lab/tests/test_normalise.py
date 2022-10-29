import pytest
import analytics.testing

from chispa.dataframe_comparer import assert_df_equality

from analytics.testing.conf_test import spark_mock_df_normalise
from analytics.testing.conf_test import spark
from analytics.lab.src.normalise import normalise
from pyspark.sql import DataFrame, SparkSession

@pytest.mark.spark
def test_normalise(spark: SparkSession, spark_mock_df_normalise):

    # ARRANGE
    df, expect = spark_mock_df_normalise

    # ACT
    actual = normalise(df)

    # ASSERT
    assert_df_equality(actual, expect)