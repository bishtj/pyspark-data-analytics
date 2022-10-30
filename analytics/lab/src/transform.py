from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(df: DataFrame) -> DataFrame:
    return df                                                                                                  \
        .drop("PrefixId", "InsSrc", "UpdSrc")                                                                   \
        .withColumn("NormalisationDate", F.lit(F.current_timestamp()))                                         \
        .withColumn("LastYear", F.date_sub(F.current_timestamp(), 365))                                        \
        .withColumn("IsNew",
                        F.when(
                                F.lit(F.datediff(F.col("InsDt"), F.col("LastYear")) > F.lit(0)), F.lit("true")
                              )
                         .otherwise(F.lit("false"))
                   )
