from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def matrix(df: DataFrame) -> DataFrame:
    return df                                      \
        .select("Prefix", "Product", "TotalIssAmt") \
        .groupBy("Product")                        \
        .pivot("Prefix")                            \
        .agg(F.sum(F.col("TotalIssAmt")))          \
        .na                                        \
        .fill(0)