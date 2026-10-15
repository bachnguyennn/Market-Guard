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
DEMO_DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "demo"


def display_alerts(
    path: str, selected_product: str, public_demo: bool = False,
    anomaly_df: pd.DataFrame | None = None,
) -> None:
    st.caption("Alerts identify statistical irregularities for further investigation.")
    if anomaly_df is None:
        try:
            anomaly_df = load_anomaly_data(path)
        except (OSError, ValueError) as error:
            st.error("The bundled demo alerts are unavailable." if public_demo else f"Unable to load anomaly data: {error}")
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


def main(public_demo: bool = False) -> None:
    st.set_page_config(page_title="MarketGuard", layout="wide")
    st.title("MarketGuard")
    st.caption("Cryptocurrency Market Surveillance")
    gold_df = None
    azure_anomalies = None
    azure_metadata = None
    gold_path = anomaly_path = ""
    if public_demo:
        try:
            azure_settings = st.secrets.get("azure")
        except FileNotFoundError:
            azure_settings = None
        if azure_settings is not None:
            from marketguard.dashboard.azure_loader import (
                AzureSnapshotError,
                clear_azure_cache,
                load_azure_snapshot,
            )

            st.info(
                "Azure analytics snapshot — not a continuous live feed. "
                "Collection and publishing run separately; this page does not confirm collector activity."
            )
            st.button("Refresh Azure data", on_click=clear_azure_cache)
            st.caption("Downloads are cached for up to 60 seconds. This does not automatically refresh the page.")
            try:
                with st.spinner("Loading Azure analytics..."):
                    gold_df, azure_anomalies, azure_metadata = load_azure_snapshot(azure_settings)
            except AzureSnapshotError as error:
                st.error(str(error))
                st.stop()
        else:
            st.info(
                "Synthetic demo snapshot — not a live market feed. "
                "BTC/ETH trades include an intentional volume spike and price jump. "
                "The ingestion and Spark pipelines run separately from this website."
            )
            gold_path = str(DEMO_DATA_PATH / "gold.parquet")
            anomaly_path = str(DEMO_DATA_PATH / "anomalies.parquet")
    else:
        gold_path = st.sidebar.text_input(
            "Gold Parquet path", value=os.getenv("MARKETGUARD_GOLD_PATH", str(DEFAULT_GOLD_PATH))
        )
        anomaly_path = st.sidebar.text_input(
            "Anomaly Parquet path", value=os.getenv("MARKETGUARD_ANOMALY_PATH", str(DEFAULT_ANOMALY_PATH))
        )

    if gold_df is None:
        try:
            gold_df = load_gold_data(gold_path)
        except (OSError, ValueError) as error:
            if public_demo:
                st.error("The bundled demo market data is unavailable.")
            else:
                st.error(f"Unable to load Gold data: {error}")
                st.info("Generate local outputs with: python -m marketguard.run_pipeline")
            st.stop()

    if gold_df.empty:
        st.info("The analytics snapshot is empty." if public_demo else "The Gold dataset is empty. Run the batch pipeline with valid trades.")
        st.stop()

    if azure_metadata is not None:
        observation = gold_df["minute_start"].max()
        age_minutes = (pd.Timestamp.now(tz="UTC") - observation).total_seconds() / 60
        st.caption(f"Last market observation (minute): {observation:%Y-%m-%d %H:%M UTC}")
        st.caption(
            f"Snapshot published: {azure_metadata['published_at']:%Y-%m-%d %H:%M:%S UTC} · "
            f"Downloaded: {azure_metadata['downloaded_at']:%Y-%m-%d %H:%M:%S UTC}"
        )
        if age_minutes < -1:
            st.warning("The source data timestamp is in the future. Check the data and clock settings.")
        else:
            st.caption(f"Source data age: {max(0, int(age_minutes))} minutes")
            if age_minutes > 30:
                st.warning("Market observations are over 30 minutes old. This is stored snapshot data, not current market activity.")

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
        display_alerts(anomaly_path, selected_product, public_demo=public_demo, anomaly_df=azure_anomalies)


if __name__ == "__main__":
    main()
