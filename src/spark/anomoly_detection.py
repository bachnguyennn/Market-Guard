
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DEFAULT_LOOKBACK_MINUTES = 60
DEFAULT_ZSCORE_THRESHOLD = 4.0
# Scale absolute deviations to match a normal standard deviation.
MAD_TO_STD = 1.4826
MEAN_AD_TO_STD = 1.253314


def _sorted_array_median(sorted_values: Column) -> Column:
    """Return the median of an ascending array, or null when it is empty."""
    size = F.size(sorted_values)
    lower = F.floor((size + 1) / 2).cast("int")
    upper = (F.floor(size / 2) + 1).cast("int")
    return F.when(
        size > 0,
        (F.element_at(sorted_values, lower) + F.element_at(sorted_values, upper)) / 2,
    )


def _with_rolling_median_and_scale(
    df: DataFrame,
    value_column: str,
    history_window,
    median_column: str,
    scale_column: str,
    count_column: str,
) -> DataFrame:
    """
    Add the median, robust scale, and non-null count of a value's history.

    The scale is the MAD, or the mean absolute deviation when tied history
    makes the MAD zero. It is only zero when the history is entirely flat.
    """
    history_column = f"_{value_column}_history"
    deviations_column = f"_{value_column}_deviations"
    history = F.col(history_column)
    deviations = F.col(deviations_column)
    mad = _sorted_array_median(deviations)
    mean_deviation = F.when(
        F.size(deviations) > 0,
        F.aggregate(deviations, F.lit(0.0), lambda total, value: total + value)
        / F.size(deviations),
    )
    return (
        df.withColumn(
            history_column,
            F.array_sort(F.collect_list(value_column).over(history_window)),
        )
        .withColumn(count_column, F.size(history))
        .withColumn(median_column, _sorted_array_median(history))
        .withColumn(
            deviations_column,
            F.array_sort(
                F.transform(history, lambda value: F.abs(value - F.col(median_column)))
            ),
        )
        .withColumn(
            scale_column,
            F.when(mad > 0, mad * F.lit(MAD_TO_STD)).otherwise(
                mean_deviation * F.lit(MEAN_AD_TO_STD)
            ),
        )
        .drop(history_column, deviations_column)
    )


def calculate_price_returns(gold_df: DataFrame) -> DataFrame:
    """
    Calculate one-minute price returns from VWAP.

    Returns are null when the previous observation is missing,
    is not exactly one minute earlier, or has a zero VWAP.
    """
    partition_columns = ["product_id"]
    if "segment_id" in gold_df.columns:
        partition_columns.append("segment_id")
    product_window = Window.partitionBy(*partition_columns).orderBy("minute_start")
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


def detect_price_jumps(
    returns_df: DataFrame,
    threshold: float = 0.02,
) -> DataFrame:
    """
    Flag absolute one-minute returns greater than the threshold.

    Null returns, including those caused by missing minutes, are
    not treated as confirmed price jumps.
    """
    if threshold < 0:
        raise ValueError("threshold must be non-negative")

    return (
        returns_df.withColumn(
            "is_price_jump",
            F.coalesce(
                F.abs(F.col("price_return")) > F.lit(threshold),
                F.lit(False),
            ),
        )
        .orderBy("product_id", "minute_start")
    )


def detect_volume_spikes(
    gold_df: DataFrame,
    lookback: int = DEFAULT_LOOKBACK_MINUTES,
    threshold: float = DEFAULT_ZSCORE_THRESHOLD,
) -> DataFrame:
    """
    Detect unusual one-minute notional volume using a robust z-score
    of log volume against its rolling historical median and scale.

    Rows without a full window of history, or with flat history, are unscored.
    """
    if lookback < 2:
        raise ValueError("lookback must be at least 2")

    partition_columns = ["product_id"]
    if "segment_id" in gold_df.columns:
        partition_columns.append("segment_id")
    history_window = (
        Window.partitionBy(*partition_columns)
        .orderBy("minute_start")
        .rowsBetween(-lookback, -1)
    )

    volume = F.col("notional_volume").cast("double")
    with_history = _with_rolling_median_and_scale(
        gold_df.withColumn("log_volume", F.when(volume > 0, F.log(volume))),
        "log_volume",
        history_window,
        "rolling_median",
        "rolling_scale",
        "history_count",
    )

    valid_history = (
        F.col("log_volume").isNotNull()
        & (F.col("history_count") >= lookback)
        & (F.col("rolling_scale") > 0)
    )

    return (
        with_history.withColumn(
            "volume_zscore",
            F.when(
                valid_history,
                (F.col("log_volume") - F.col("rolling_median"))
                / F.col("rolling_scale"),
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

    partition_columns = ["product_id"]
    if "segment_id" in returns_df.columns:
        partition_columns.append("segment_id")
    history_window = (
        Window.partitionBy(*partition_columns)
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


def detect_statistical_price_jumps(
    returns_df: DataFrame,
    lookback: int = DEFAULT_LOOKBACK_MINUTES,
    threshold: float = DEFAULT_ZSCORE_THRESHOLD,
) -> DataFrame:
    """Detect unusually large returns using a rolling robust z-score."""
    if lookback < 2:
        raise ValueError("lookback must be at least 2")
    if threshold < 0:
        raise ValueError("threshold must be non-negative")

    partition_columns = ["product_id"]
    if "segment_id" in returns_df.columns:
        partition_columns.append("segment_id")
    history_window = (
        Window.partitionBy(*partition_columns)
        .orderBy("minute_start")
        .rowsBetween(-lookback, -1)
    )

    with_history = _with_rolling_median_and_scale(
        returns_df.withColumn("_return", F.col("price_return").cast("double")),
        "_return",
        history_window,
        "rolling_median_return",
        "rolling_scale_return",
        "return_history_count",
    ).drop("_return")

    valid_history = (
        F.col("price_return").isNotNull()
        & (F.col("return_history_count") >= lookback)
        & (F.col("rolling_scale_return") > 0)
    )

    return (
        with_history.withColumn(
            "return_zscore",
            F.when(
                valid_history,
                (F.col("price_return").cast("double") - F.col("rolling_median_return"))
                / F.col("rolling_scale_return"),
            ),
        )
        .withColumn(
            "is_statistical_price_jump",
            F.coalesce(
                F.abs(F.col("return_zscore")) > F.lit(threshold),
                F.lit(False),
            ),
        )
        .orderBy("product_id", "minute_start")
    )
