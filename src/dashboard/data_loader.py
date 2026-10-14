from pathlib import Path

import pandas as pd

GOLD_COLUMNS = {
    "product_id",
    "minute_start",
    "trade_count",
    "notional_volume",
    "vwap",
}
ALERT_EXPLANATIONS = {
    "VOLUME": "Trading volume exceeded its historical threshold",
    "PRICE": "Price return deviated unusually from historical behavior",
    "BOTH": "Both volume and price indicators triggered",
}


def _load_parquet(path: str, required_columns: set[str], label: str) -> pd.DataFrame:
    parquet_path = Path(path)
    if not parquet_path.exists():
        raise FileNotFoundError(f"{label} Parquet dataset not found: {parquet_path}")

    data = pd.read_parquet(parquet_path, engine="pyarrow")
    missing_columns = required_columns - set(data.columns)
    if missing_columns:
        raise ValueError(
            f"{label} dataset is missing required columns: "
            f"{', '.join(sorted(missing_columns))}"
        )

    data["minute_start"] = pd.to_datetime(data["minute_start"], errors="raise", utc=True)
    if data["minute_start"].isna().any():
        raise ValueError(f"{label} dataset contains missing minute_start values")

    return data.sort_values(["minute_start", "product_id"]).reset_index(drop=True)


def load_gold_data(path: str) -> pd.DataFrame:
    """Read validated Gold Parquet without converting financial Decimal values."""
    return _load_parquet(path, GOLD_COLUMNS, "Gold")


def _boolean_flag(data: pd.DataFrame, column: str) -> pd.Series:
    try:
        return data[column].astype("boolean").fillna(False)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Anomaly column {column} must contain Boolean values") from error


def load_anomaly_data(path: str) -> pd.DataFrame:
    """Load anomaly Parquet with UTC timestamps and a combined Boolean flag."""
    data = _load_parquet(path, {"product_id", "minute_start"}, "Anomaly")
    signal_columns = {"is_volume_spike", "is_statistical_price_jump"}
    for column in signal_columns.intersection(data.columns):
        data[column] = _boolean_flag(data, column)
    if "is_anomaly" not in data.columns:
        if not signal_columns.issubset(data.columns):
            raise ValueError(
                "Anomaly dataset is missing required columns: is_anomaly "
                "(or both is_volume_spike and is_statistical_price_jump)"
            )
        data["is_anomaly"] = (
            data["is_volume_spike"] | data["is_statistical_price_jump"]
        )
    else:
        data["is_anomaly"] = _boolean_flag(data, "is_anomaly")
    if "anomaly_type" not in data.columns and signal_columns.issubset(data.columns):
        data["anomaly_type"] = (
            pd.Series("NONE", index=data.index)
            .mask(data["is_volume_spike"], "VOLUME")
            .mask(data["is_statistical_price_jump"], "PRICE")
            .mask(data["is_volume_spike"] & data["is_statistical_price_jump"], "BOTH")
        )
    return data


def filter_by_product(
    data: pd.DataFrame,
    selected_product: str = "All products",
) -> pd.DataFrame:
    """Return a copy containing the selected product's records."""
    if selected_product == "All products":
        return data.copy()
    return data.loc[data["product_id"] == selected_product].copy()


def select_alerts(
    data: pd.DataFrame,
    selected_product: str = "All products",
) -> pd.DataFrame:
    """Return flagged records, newest first, with statistical explanations."""
    filtered = filter_by_product(data, selected_product)
    alerts = filtered.loc[filtered["is_anomaly"].eq(True).fillna(False)].copy()
    if "anomaly_type" in alerts.columns:
        alerts["explanation"] = alerts["anomaly_type"].map(ALERT_EXPLANATIONS)
    else:
        alerts["explanation"] = pd.Series(index=alerts.index, dtype="string")
    alerts["explanation"] = alerts["explanation"].fillna(
        "A statistical anomaly indicator triggered"
    )
    return alerts.sort_values(
        ["minute_start", "product_id"], ascending=[False, True]
    ).reset_index(drop=True)
