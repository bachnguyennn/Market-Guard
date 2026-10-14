from pathlib import Path

import pandas as pd
import streamlit as st

DEFAULT_GOLD_PATH = Path("data/batch_output/gold")
REQUIRED_COLUMNS = {
    "product_id",
    "minute_start",
    "trade_count",
    "notional_volume",
    "vwap",
}


def load_gold_data(path: str) -> pd.DataFrame:
    """Read Gold Parquet, validate its columns, and normalize timestamps to UTC."""
    parquet_path = Path(path)
    if not parquet_path.exists():
        raise FileNotFoundError(f"Gold Parquet dataset not found: {parquet_path}")

    gold_df = pd.read_parquet(parquet_path, engine="pyarrow")
    missing_columns = REQUIRED_COLUMNS - set(gold_df.columns)
    if missing_columns:
        raise ValueError(
            f"Gold dataset is missing required columns: {', '.join(sorted(missing_columns))}"
        )

    gold_df["minute_start"] = pd.to_datetime(
        gold_df["minute_start"],
        errors="raise",
        utc=True,
    )
    if gold_df["minute_start"].isna().any():
        raise ValueError("Gold dataset contains missing minute_start values")

    return gold_df.sort_values(["minute_start", "product_id"]).reset_index(drop=True)


def main() -> None:
    st.set_page_config(page_title="MarketGuard", layout="wide")
    st.title("MarketGuard")
    st.caption("Cryptocurrency Market Surveillance")
    gold_path = st.sidebar.text_input("Gold Parquet path", value=str(DEFAULT_GOLD_PATH))

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
    filtered_df = gold_df
    if selected_product != "All products":
        filtered_df = gold_df.loc[gold_df["product_id"] == selected_product]

    st.metric("Total trades", f"{int(filtered_df['trade_count'].sum()):,}")
    st.subheader("Notional volume by minute")
    chart_df = filtered_df[["minute_start", "product_id", "notional_volume"]].copy()
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


if __name__ == "__main__":
    main()
