import os
from pathlib import Path

import pandas as pd
import streamlit as st
from marketguard.dashboard.data_loader import (
    filter_by_product,
    load_anomaly_data,
    load_gold_data,
    select_alerts,
)

DEFAULT_GOLD_PATH = Path("data/batch_output/gold")
DEFAULT_ANOMALY_PATH = Path("data/batch_output/anomalies")


def display_alerts(path: str, selected_product: str) -> None:
    st.caption("Alerts identify statistical irregularities for further investigation.")
    try:
        anomaly_df = load_anomaly_data(path)
    except (OSError, ValueError) as error:
        st.error(f"Unable to load anomaly data: {error}")
        return

    alerts_df = select_alerts(anomaly_df, selected_product)
    st.metric("Total alerts", f"{len(alerts_df):,}")
    if alerts_df.empty:
        st.info("No anomalies detected in the selected period.")
        return

    display_columns = [
        "minute_start",
        "product_id",
        "anomaly_type",
        "volume_zscore",
        "return_zscore",
        "is_anomaly",
        "explanation",
    ]
    st.dataframe(
        alerts_df[[column for column in display_columns if column in alerts_df.columns]],
        width="stretch",
        hide_index=True,
    )


def main() -> None:
    st.set_page_config(page_title="MarketGuard", layout="wide")
    st.title("MarketGuard")
    st.caption("Cryptocurrency Market Surveillance")
    gold_path = st.sidebar.text_input(
        "Gold Parquet path", value=os.getenv("MARKETGUARD_GOLD_PATH", str(DEFAULT_GOLD_PATH))
    )
    anomaly_path = st.sidebar.text_input(
        "Anomaly Parquet path", value=os.getenv("MARKETGUARD_ANOMALY_PATH", str(DEFAULT_ANOMALY_PATH))
    )

    try:
        gold_df = load_gold_data(gold_path)
    except (OSError, ValueError) as error:
        st.error(f"Unable to load Gold data: {error}")
        st.info("Generate local outputs with: python -m marketguard.run_pipeline")
        st.stop()

    if gold_df.empty:
        st.info("The Gold dataset is empty. Run the batch pipeline with valid trades.")
        st.stop()

    products = sorted(gold_df["product_id"].dropna().unique().tolist())
    selected_product = st.selectbox("Product", ["All products", *products])
    filtered_df = filter_by_product(gold_df, selected_product)
    overview_tab, alerts_tab = st.tabs(["Market Overview", "Anomaly Alerts"])

    with overview_tab:
        st.metric("Total trades", f"{int(filtered_df['trade_count'].sum()):,}")
        st.subheader("Notional volume by minute")
        chart_df = filtered_df[["minute_start", "product_id", "notional_volume"]].copy()
        chart_df["minute_start"] = chart_df["minute_start"].dt.strftime("%Y-%m-%d %H:%M")
        chart_df["notional_volume"] = pd.to_numeric(chart_df["notional_volume"])
        st.bar_chart(
            chart_df,
            x="minute_start",
            y="notional_volume",
            color="product_id",
            x_label="Minute (UTC)",
            y_label="Notional volume (USD)",
        )
        st.caption("Saved batch data. All times are UTC.")
        st.dataframe(
            filtered_df[
                ["product_id", "minute_start", "trade_count", "notional_volume", "vwap"]
            ],
            hide_index=True,
        )

    with alerts_tab:
        display_alerts(anomaly_path, selected_product)


if __name__ == "__main__":
    main()
