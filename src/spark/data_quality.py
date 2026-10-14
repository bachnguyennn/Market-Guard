
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def detect_missing_minutes(
    gold_df: DataFrame,
) -> DataFrame:
    """
    Identify gaps between consecutive
    observed minute buckets per product.
    """
    product_window = Window.partitionBy("product_id").orderBy("minute_start")

    with_previous = gold_df.withColumn(
        "previous_minute",
        F.lag("minute_start").over(product_window),
    )

    with_elapsed_time = with_previous.withColumn(
        "seconds_since_previous",
        F.col("minute_start").cast("long")
        - F.col("previous_minute").cast("long"),
    )

    return (
        with_elapsed_time.withColumn(
            "missing_minutes",
            F.greatest(
                (F.col("seconds_since_previous") / 60).cast("long") - 1,
                F.lit(0),
            ),
        )
        .withColumn("is_data_gap", F.col("missing_minutes") > 0)
        .orderBy("product_id", "minute_start")
    )
def assign_continuous_segments(
    df: DataFrame,
) -> DataFrame:
    """
    Assign a segment ID for each continuous
    sequence of observed one-minute buckets.

    Requires:
    product_id, minute_start, is_data_gap
    """
    cumulative_window = (
        Window.partitionBy("product_id")
        .orderBy("minute_start")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return (
        df.withColumn(
            "gap_start",
            F.when(F.col("is_data_gap"), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn("segment_id", F.sum("gap_start").over(cumulative_window))
        .orderBy("product_id", "minute_start")
    )
