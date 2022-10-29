import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, DoubleType, \
    DecimalType
from analytics.lab.src.common.defines import RAW_SCHEMA, NORMALISE_SCHEMA
from datetime import date, datetime
from pyspark.sql import DataFrame, SparkSession
from decimal import Context


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName("pytest-spark").getOrCreate()


def decimal(precision, value):
    return Context(prec=precision).create_decimal(value)


@pytest.fixture
def spark_mock_df_normalise(spark: SparkSession) -> (DataFrame, DataFrame):
    input = spark.createDataFrame(
        data=[
            (
                '3256759', 'GNM   672M', 'POOL', 'GNMII30M', '36202AW94', 'GNM', 'SF', '135', '550190.', 'LOAN',
                '19930101',
                '20220720', '7.189', '354', '1', '19000101', '3256759', '-999', 'Y', '#', 'G23', '19940101 00:00:00',
                'GN_B', '20170508 19:49:00')
        ],
        schema=RAW_SCHEMA
    )

    expect = spark.createDataFrame(
        data=[
            ('GNM   672M', 'POOL', 'GNMII30M', '36202AW94', 'GNM', 'SF', 'LOAN', 'Y', '#', 'G23', 'GN_B', 3256759, 135,
             550190.0, date(1993, 1, 1), date(2022, 7, 20), 7.189, 354, 1, date(1900, 1, 1), 3256759, -999,
             datetime(1994, 1, 1, 00, 00, 00), datetime(2017, 5, 8, 19, 49, 00))
        ],
        schema=NORMALISE_SCHEMA
    )

    return input, expect


@pytest.fixture
def spark_mock_df_transform(spark: SparkSession) -> (DataFrame, DataFrame):
    input = spark.createDataFrame(
        data=[
            (11111, 'insSrc1', 'updSrc1', datetime(1994, 1, 1, 00, 00, 00)),
            (22222, 'insSrc2', 'updSrc2', datetime(2022, 1, 1, 00, 00, 00)),
        ],
        schema=StructType([
            StructField("PrefixId", IntegerType()),
            StructField("InsSrc", StringType()),
            StructField("UpdSrc", StringType()),
            StructField("InsDt", TimestampType())
        ])
    )

    expect = spark.createDataFrame(
        data=[
            (datetime(2022, 1, 1, 00, 00, 00), date(2012, 10, 31), "true"),
            (datetime(1994, 1, 1, 00, 00, 00), date(2012, 10, 31), "false")
        ],
        schema=StructType([
            StructField("InsDt", TimestampType()),
            StructField("LastYear", DateType(), False),
            StructField("IsNew", StringType(), False)
        ])
    )
    return input, expect

@pytest.fixture
def spark_mock_df_aggregate(spark: SparkSession) -> (DataFrame, DataFrame):
    input = spark.createDataFrame(
        data=[
            ('SF', 'Product1', datetime(2001, 1, 1, 00, 00, 00), 100.11),
            ('SF', 'Product1', datetime(2001, 2, 2, 00, 00, 00), 200.22),
            ('SF', 'Product1', datetime(2002, 3, 3, 00, 00, 00), 300.33),
            ('SF', 'Product1', datetime(2002, 4, 4, 00, 00, 00), 500.50),
            ('AF', 'ProductA', datetime(2010, 1, 1, 00, 00, 00), 10.10),
            ('AF', 'ProductA', datetime(2010, 1, 1, 00, 00, 00), 20.20),
            ('AF', 'ProductB', datetime(2010, 1, 1, 00, 00, 00), 10.10),
            ('AF', 'ProductB', datetime(2020, 1, 1, 00, 00, 00), 33.33),
            ('AF', 'ProductB', datetime(2020, 1, 1, 00, 00, 00), 55.55),
        ],
        schema=StructType([
            StructField("Prefix", StringType()),
            StructField("Product", StringType()),
            StructField("IssDt", DateType()),
            StructField("IssAmt", DoubleType())
        ])
    )

    expect = spark.createDataFrame(
        data=[
            ('SF', 'Product1', 2002, decimal(10, 300), decimal(10, 501), decimal(10, 801)),
            ('AF', 'ProductA', 2010, decimal(10, 10), decimal(10, 20), decimal(10, 30)),
            ('AF', 'ProductB', 2020, decimal(10, 33), decimal(10, 56), decimal(10, 89)),
            ('AF', 'ProductB', 2010, decimal(10, 10), decimal(10, 10), decimal(10, 10)),
            ('SF', 'Product1', 2001, decimal(10, 100), decimal(10, 200), decimal(10, 300)),
        ],
        schema=StructType([
            StructField("Prefix", StringType()),
            StructField("Product", StringType()),
            StructField("IssYear", IntegerType()),
            StructField("MinIssAmt", DecimalType(10, 0)),
            StructField("MaxIssAmt", DecimalType(10, 0)),
            StructField("TotalIssAmt", DecimalType(10, 0))
        ])
    )

    return input, expect


@pytest.fixture
def spark_mock_df_matrix(spark: SparkSession) -> (DataFrame, DataFrame):
    input = spark.createDataFrame(
        data=[
            ('SF', 'GNMII30M', decimal(10, 100)),
            ('SF', 'GNMII30M', decimal(10, 200)),
            ('SP', 'GNM30', decimal(10, 11)),
            ('AF', 'GNM30', decimal(10, 140)),
            ('AF', 'GNM31', decimal(10, 420)),
        ],
        schema=StructType([
            StructField("Prefix", StringType()),
            StructField("Product", StringType()),
            StructField("TotalIssAmt", DecimalType(10, 0))
        ])
    )

    expect = spark.createDataFrame(
        data=[
            ('GNM31', decimal(20, 420), decimal(20, 0), decimal(20, 0)),
            ('GNM30', decimal(20, 140), decimal(20, 0), decimal(20, 11)),
            ('GNMII30M', decimal(20, 0), decimal(20, 300), decimal(20, 0)),
        ],
        schema=StructType([
            StructField("Product", StringType()),
            StructField("AF", DecimalType(20, 0)),
            StructField("SF", DecimalType(20, 0)),
            StructField("SP", DecimalType(20, 0)),

        ])
    )

    return input, expect
