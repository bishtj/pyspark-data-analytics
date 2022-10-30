from pyspark.sql import SparkSession
from analytics.lab.src.common.defines import RAW_SCHEMA
from analytics.lab.src.normalise import normalise
from analytics.lab.src.transform import transform
from analytics.lab.src.aggregate import aggregate
from analytics.lab.src.matrix import matrix

if __name__ == '__main__':
    spark = SparkSession                              \
        .builder                                      \
        .appName("Secure application")                \
        .master("local[2]")                           \
        .getOrCreate()                                \

    df_input = spark                                  \
        .read                                         \
        .option("delimiter", "|")                     \
        .schema(RAW_SCHEMA)                           \
        .csv("analytics/lab/resources/input.dat")

    df_normalise = normalise(df_input)

    df_transform = transform(df_normalise)

    df_agg = aggregate(df_transform)

    df_matrix = matrix(df_agg)

    df_cached = df_matrix.cache()

    df_cached.coalesce(1).write.mode('overwrite').parquet("results")

    df_cached.show()

    df_cached.unpersist()