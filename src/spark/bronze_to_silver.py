from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType


def transform_trades(df: DataFrame) -> DataFrame:
    """
    Convert raw Coinbase trade data
    into validated Silver records.
    """

    return (
        df.filter(F.col("type") == "match")
        .withColumn("price", F.col("price").cast(DecimalType(20, 8)))
        .withColumn("size", F.col("size").cast(DecimalType(20, 8)))
        .withColumn("trade_id", F.col("trade_id").cast(LongType()))
        .withColumn("time", F.to_timestamp("time"))
        .filter(
            F.col("trade_id").isNotNull()
            & F.col("product_id").isNotNull()
            & (F.col("price") > 0)
            & (F.col("size") > 0)
            & F.col("time").isNotNull()
        )
        .dropDuplicates(["product_id", "trade_id"])
    )


def main():
    spark = (
        SparkSession.builder
        .appName("MarketGuard-BronzeToSilver")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        bronze_df = spark.read.json("data/spark_input")
        silver_df = transform_trades(bronze_df)

        silver_df.printSchema()
        silver_count = silver_df.count()
        print(f"Silver records before write: {silver_count}")

        silver_df.write.mode("overwrite").parquet("data/silver")

        saved_silver_df = spark.read.parquet("data/silver")
        saved_count = saved_silver_df.count()
        print(f"Silver records after read: {saved_count}")

        if saved_count != silver_count:
            raise RuntimeError(
                "Silver record count changed after writing Parquet"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
