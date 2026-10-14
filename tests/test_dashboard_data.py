from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from marketguard.dashboard import app
from marketguard.dashboard.data_loader import (
    ALERT_EXPLANATIONS,
    filter_by_product,
    load_anomaly_data,
    load_gold_data,
    select_alerts,
)


@pytest.fixture
def anomaly_df():
    return pd.DataFrame(
        {
            "product_id": ["BTC-USD", "ETH-USD", "BTC-USD", "BTC-USD", "ETH-USD"],
            "minute_start": pd.date_range("2026-10-14T12:00:00Z", periods=5, freq="min"),
            "is_anomaly": [True, True, False, True, False],
            "anomaly_type": ["VOLUME", "PRICE", "NONE", "BOTH", "NONE"],
            "volume_zscore": [5.0, 0.1, 0.0, 4.0, 0.0],
            "return_zscore": [0.1, -4.0, 0.0, 5.0, 0.0],
        }
    )


def test_load_anomaly_data_reads_parquet_dataset(tmp_path, anomaly_df):
    path = tmp_path / "anomalies"
    path.mkdir()
    anomaly_df.iloc[:2].to_parquet(path / "part-00000.parquet", index=False)
    anomaly_df.iloc[2:].to_parquet(path / "part-00001.parquet", index=False)
    (path / "_SUCCESS").touch()

    loaded = load_anomaly_data(str(path))

    assert len(loaded) == 5
    assert str(loaded["minute_start"].dt.tz) == "UTC"
    assert loaded["is_anomaly"].sum() == 3
    assert set(loaded.columns) == set(anomaly_df.columns)


@pytest.mark.parametrize("loader", [load_gold_data, load_anomaly_data])
def test_loaders_report_missing_dataset(tmp_path, loader):
    with pytest.raises(FileNotFoundError, match="Parquet dataset not found"):
        loader(str(tmp_path / "missing"))


@pytest.mark.parametrize("missing_column", ["product_id", "minute_start", "is_anomaly"])
def test_anomaly_loader_rejects_invalid_schema(tmp_path, anomaly_df, missing_column):
    path = tmp_path / "anomalies.parquet"
    anomaly_df.drop(columns=missing_column).to_parquet(path, index=False)

    with pytest.raises(ValueError, match=missing_column):
        load_anomaly_data(str(path))


def test_anomaly_loader_combines_detector_flags(tmp_path):
    data = pd.DataFrame(
        {
            "product_id": ["BTC-USD"] * 4,
            "minute_start": pd.date_range("2026-10-14T12:00:00Z", periods=4, freq="min"),
            "is_volume_spike": [True, False, False, None],
            "is_statistical_price_jump": [False, True, False, None],
        }
    )
    path = tmp_path / "anomalies.parquet"
    data.to_parquet(path, index=False)

    loaded = load_anomaly_data(str(path))

    assert loaded["is_anomaly"].tolist() == [True, True, False, False]
    assert loaded["anomaly_type"].tolist() == ["VOLUME", "PRICE", "NONE", "NONE"]
    assert len(select_alerts(loaded)) == 2
    assert select_alerts(loaded)["explanation"].tolist() == [
        ALERT_EXPLANATIONS["PRICE"],
        ALERT_EXPLANATIONS["VOLUME"],
    ]


def test_anomaly_loader_rejects_text_flags(tmp_path, anomaly_df):
    path = tmp_path / "anomalies.parquet"
    anomaly_df["is_anomaly"] = ["False"] * len(anomaly_df)
    anomaly_df.to_parquet(path, index=False)

    with pytest.raises(ValueError, match="must contain Boolean values"):
        load_anomaly_data(str(path))


@pytest.mark.parametrize("product, count", [("All products", 3), ("BTC-USD", 2), ("ETH-USD", 1)])
def test_select_alerts_filters_flags_and_products_newest_first(anomaly_df, product, count):
    original = anomaly_df.copy(deep=True)

    alerts = select_alerts(anomaly_df, product)

    assert len(alerts) == count
    assert alerts["is_anomaly"].all()
    assert alerts["minute_start"].is_monotonic_decreasing
    assert alerts["explanation"].tolist() == [
        ALERT_EXPLANATIONS[value] for value in alerts["anomaly_type"]
    ]
    if product != "All products":
        assert set(alerts["product_id"]) == {product}
        assert set(filter_by_product(anomaly_df, product)["product_id"]) == {product}
    pd.testing.assert_frame_equal(anomaly_df, original)


def write_dashboard_data(tmp_path, anomaly_df):
    gold_path = tmp_path / app.DEFAULT_GOLD_PATH
    anomaly_path = tmp_path / app.DEFAULT_ANOMALY_PATH
    gold_path.mkdir(parents=True)
    anomaly_path.mkdir(parents=True)
    gold_df = anomaly_df[["product_id", "minute_start"]].assign(
        trade_count=1, notional_volume=100.0, vwap=100.0
    )
    gold_df.to_parquet(gold_path / "part-00000.parquet", index=False)
    anomaly_df.to_parquet(anomaly_path / "part-00000.parquet", index=False)


def test_dashboard_alert_tab_shares_product_filter(tmp_path, monkeypatch, anomaly_df):
    monkeypatch.chdir(tmp_path)
    write_dashboard_data(tmp_path, anomaly_df)

    page = AppTest.from_file(str(Path(app.__file__).resolve()), default_timeout=20).run()

    assert not page.exception
    assert [tab.label for tab in page.tabs] == ["Market Overview", "Anomaly Alerts"]
    alerts_tab = page.tabs[1]
    assert alerts_tab.metric[0].value == "3"
    assert len(alerts_tab.dataframe[0].value) == 3
    assert alerts_tab.dataframe[0].value["minute_start"].is_monotonic_decreasing
    assert "explanation" in alerts_tab.dataframe[0].value.columns

    for product, expected_count in [("BTC-USD", "2"), ("ETH-USD", "1")]:
        page.selectbox[0].select(product).run()
        assert not page.exception
        assert page.tabs[1].metric[0].value == expected_count
        assert set(page.tabs[1].dataframe[0].value["product_id"]) == {product}


@pytest.mark.parametrize("empty_dataset", [False, True])
def test_dashboard_empty_alerts_are_not_counted_as_anomalies(
    tmp_path, monkeypatch, anomaly_df, empty_dataset
):
    monkeypatch.chdir(tmp_path)
    anomaly_df["is_anomaly"] = False
    write_dashboard_data(tmp_path, anomaly_df)
    if empty_dataset:
        anomaly_path = tmp_path / app.DEFAULT_ANOMALY_PATH / "part-00000.parquet"
        anomaly_df.iloc[:0].to_parquet(anomaly_path, index=False)

    page = AppTest.from_file(str(Path(app.__file__).resolve()), default_timeout=20).run()

    assert not page.exception
    assert page.tabs[1].metric[0].value == "0"
    assert page.tabs[1].info[0].value == "No anomalies detected in the selected period."
    assert not page.tabs[1].dataframe


def test_missing_anomalies_does_not_hide_market_overview(tmp_path, monkeypatch, anomaly_df):
    monkeypatch.chdir(tmp_path)
    gold_path = tmp_path / app.DEFAULT_GOLD_PATH
    gold_path.mkdir(parents=True)
    anomaly_df[["product_id", "minute_start"]].assign(
        trade_count=1, notional_volume=100.0, vwap=100.0
    ).to_parquet(gold_path / "part-00000.parquet", index=False)

    page = AppTest.from_file(str(Path(app.__file__).resolve()), default_timeout=20).run()

    assert not page.exception
    assert page.tabs[0].metric[0].value == "5"
    assert "Anomaly Parquet dataset not found" in page.tabs[1].error[0].value
