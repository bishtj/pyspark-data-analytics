from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

RAW_SCHEMA = StructType([
    StructField("SecId", StringType()),
    StructField("SecMnem", StringType()),
    StructField("ProductSupType", StringType()),
    StructField("Product", StringType()),
    StructField("CUSIP", StringType()),
    StructField("Agency", StringType()),
    StructField("Prefix", StringType()),
    StructField("PrefixId", StringType()),
    StructField("IssAmt", StringType()),
    StructField("CollType", StringType()),
    StructField("IssDt", StringType()),
    StructField("MatDt", StringType()),
    StructField("OrigWac", StringType()),
    StructField("OrigWam", StringType()),
    StructField("OrigWala", StringType()),
    StructField("RetireDt", StringType()),
    StructField("IssueId", StringType()),
    StructField("PriceId", StringType()),
    StructField("TBAEligCode", StringType()),
    StructField("SsnCode", StringType()),
    StructField("InsSrc", StringType()),
    StructField("InsDt", StringType()),
    StructField("UpdSrc", StringType()),
    StructField("UpdDt", StringType()),
])

NORMALISE_SCHEMA = StructType([
    StructField("SecMnem", StringType()),
    StructField("ProductSupType", StringType()),
    StructField("Product", StringType()),
    StructField("CUSIP", StringType()),
    StructField("Agency", StringType()),
    StructField("Prefix", StringType()),
    StructField("CollType", StringType()),
    StructField("TBAEligCode", StringType()),
    StructField("SsnCode", StringType()),
    StructField("InsSrc", StringType()),
    StructField("UpdSrc", StringType()),
    StructField("SecId", IntegerType()),
    StructField("PrefixId", IntegerType()),
    StructField("IssAmt", DoubleType()),
    StructField("IssDt", DateType()),
    StructField("MatDt", DateType()),
    StructField("OrigWac", DoubleType()),
    StructField("OrigWam", IntegerType()),
    StructField("OrigWala", IntegerType()),
    StructField("RetireDt", DateType()),
    StructField("IssueId", IntegerType()),
    StructField("PriceId", IntegerType()),
    StructField("InsDt", TimestampType()),
    StructField("UpdDt", TimestampType()),
]
)