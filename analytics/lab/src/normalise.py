from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


def normalise(inputDf: DataFrame) -> DataFrame:
    return inputDf.withColumn("SecId_new", F.col("SecId").cast(IntegerType()))          \
        .drop("SecId")                                                                  \
        .withColumnRenamed("SecId_new", "SecId")                                        \
        .withColumn("PrefixId_new", F.col("PrefixId").cast(IntegerType()))                \
        .drop("PrefixId")                                                                \
        .withColumnRenamed("PrefixId_new", "PrefixId")                                    \
        .withColumn("IssAmt_new", F.col("IssAmt").cast(DoubleType()))                   \
        .drop("IssAmt")                                                                 \
        .withColumnRenamed("IssAmt_new", "IssAmt")                                      \
        .withColumn("IssDt_new", F.to_date(F.col("IssDt"), "yyyyMMdd"))                 \
        .drop("IssDt")                                                                  \
        .withColumnRenamed("IssDt_new", "IssDt")                                        \
        .withColumn("MatDt_new", F.to_date(F.col("MatDt"), "yyyyMMdd"))                 \
        .drop("MatDt")                                                                  \
        .withColumnRenamed("MatDt_new", "MatDt")                                        \
        .withColumn("OrigWac_new", F.col("OrigWac").cast(DoubleType()))                 \
        .drop("OrigWac")                                                                \
        .withColumnRenamed("OrigWac_new", "OrigWac")                                    \
        .withColumn("OrigWam_new", F.col("OrigWam").cast(IntegerType()))                \
        .drop("OrigWam")                                                                \
        .withColumnRenamed("OrigWam_new", "OrigWam")                                    \
        .withColumn("OrigWala_new", F.col("OrigWala").cast(IntegerType()))              \
        .drop("OrigWala")                                                               \
        .withColumnRenamed("OrigWala_new", "OrigWala")                                  \
        .withColumn("RetireDt_new", F.to_date(F.col("RetireDt"), "yyyyMMdd"))           \
        .drop("RetireDt")                                                               \
        .withColumnRenamed("RetireDt_new", "RetireDt")                                  \
        .withColumn("IssueId_new", F.col("IssueId").cast(IntegerType()))                \
        .drop("IssueId")                                                                \
        .withColumnRenamed("IssueId_new", "IssueId")                                    \
        .withColumn("PriceId_new", F.col("PriceId").cast(IntegerType()))                \
        .drop("PriceId")                                                                \
        .withColumnRenamed("PriceId_new", "PriceId")                                    \
        .withColumn("InsDt_new", F.to_timestamp(F.col("InsDt"), "yyyyMMdd HH:mm:ss"))   \
        .drop("InsDt")                                                                  \
        .withColumnRenamed("InsDt_new", "InsDt")                                        \
        .withColumn("UpdDt_new", F.to_timestamp(F.col("UpdDt"), "yyyyMMdd HH:mm:ss"))   \
        .drop("UpdDt")                                                                  \
        .withColumnRenamed("UpdDt_new", "UpdDt")
