from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def aggregate(df: DataFrame) -> DataFrame:
    df_pre_agg = df                                                                 \
        .select("Prefix", "Product", "IssDt", "IssAmt")                              \
        .withColumn("IssYear", F.year(F.col("IssDt")))                              \
        .drop("IssDt")

    return df_pre_agg.groupBy("Prefix", "Product", "IssYear")                        \
        .agg(
             F.min(F.col("IssAmt")).cast(DecimalType(10, 0)).alias("MinIssAmt"),
             F.max(F.col("IssAmt")).cast(DecimalType(10, 0)).alias("MaxIssAmt"),
             F.sum(F.col("IssAmt")).cast(DecimalType(10, 0)).alias("TotalIssAmt")
    )
