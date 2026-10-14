
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def detect_volume_spikes(
    gold_df: DataFrame,
    lookback: int = 5,
    threshold: float = 3.0,
) -> DataFrame:
    """
    Detect unusual one-minute notional volume
    using a rolling historical z-score.
    """

    # TODO 1:
    # Create a Window partitioned by product_id
    # and ordered by minute_start.
    #
    # Use the previous `lookback` rows,
    # excluding the current row.

    # TODO 2:
    # Add rolling_mean using F.avg().
    #
    # Hint: F.avg("notional_volume").over(...)

    # TODO 3:
    # Add rolling_std using F.stddev_samp().

    # TODO 4:
    # Count historical observations.
    # Require at least `lookback` observations
    # before evaluating an anomaly.

    # TODO 5:
    # Compute the volume_zscore:
    #
    # (notional_volume - rolling_mean)
    # / rolling_std
    #
    # Handle null or zero standard deviation.

    # TODO 6:
    # Create is_volume_spike:
    # True when z-score > threshold.
    #
    # Insufficient history should NOT
    # be treated as a confirmed anomaly.

    pass
