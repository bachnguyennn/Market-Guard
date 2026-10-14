
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def calculate_price_returns(gold_df: DataFrame) -> DataFrame:
    """
    Calculate one-minute price returns from VWAP.

    Returns are null when the previous observation is missing,
    is not exactly one minute earlier, or has a zero VWAP.
    """
    product_window = Window.partitionBy("product_id").orderBy("minute_start")
    previous_vwap = F.lag("vwap").over(product_window)
    previous_minute = F.lag("minute_start").over(product_window)
    is_next_minute = (
        F.col("minute_start").cast("long")
        - previous_minute.cast("long")
        == 60
    )

    return (
        gold_df.withColumn("previous_vwap", previous_vwap)
        .withColumn(
            "price_return",
            F.when(
                is_next_minute
                & F.col("previous_vwap").isNotNull()
                & (F.col("previous_vwap") != 0),
                (F.col("vwap") - F.col("previous_vwap"))
                / F.col("previous_vwap"),
            ),
        )
        .orderBy("product_id", "minute_start")
    )


def detect_volume_spikes(
    gold_df: DataFrame,
    lookback: int = 5,
    threshold: float = 3.0,
) -> DataFrame:
    """
    Detect unusual one-minute notional volume
    using a rolling historical z-score.
    """
    if lookback < 2:
        raise ValueError("lookback must be at least 2")

    history_window = (
        Window.partitionBy("product_id")
        .orderBy("minute_start")
        .rowsBetween(-lookback, -1)
    )

    with_history = (
        gold_df.withColumn(
            "rolling_mean",
            F.avg("notional_volume").over(history_window),
        )
        .withColumn(
            "rolling_std",
            F.stddev_samp("notional_volume").over(history_window),
        )
        .withColumn(
            "history_count",
            F.count("notional_volume").over(history_window),
        )
    )

    valid_history = (
        (F.col("history_count") >= lookback)
        & F.col("rolling_std").isNotNull()
        & (F.col("rolling_std") > 0)
    )

    return (
        with_history.withColumn(
            "volume_zscore",
            F.when(
                valid_history,
                (F.col("notional_volume") - F.col("rolling_mean"))
                / F.col("rolling_std"),
            ),
        )
        .withColumn(
            "is_volume_spike",
            F.coalesce(F.col("volume_zscore") > F.lit(threshold), F.lit(False)),
        )
        .orderBy("product_id", "minute_start")
    )


def calculate_rolling_volatility(
    returns_df: DataFrame,
    lookback: int = 5,
) -> DataFrame:
    """
    Estimate historical volatility using
    the previous N valid one-minute returns.
    """
    if lookback < 2:
        raise ValueError("lookback must be at least 2")

    history_window = (
        Window.partitionBy("product_id")
        .orderBy("minute_start")
        .rowsBetween(-lookback, -1)
    )

    with_history = (
        returns_df.withColumn(
            "rolling_volatility",
            F.stddev_samp("price_return").over(history_window),
        )
        .withColumn(
            "return_history_count",
            F.count("price_return").over(history_window),
        )
    )

    return (
        with_history.withColumn(
            "rolling_volatility",
            F.when(
                F.col("return_history_count") >= lookback,
                F.col("rolling_volatility"),
            ),
        )
        .orderBy("product_id", "minute_start")
    )

