from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest
from marketguard.dashboard import app
from streamlit.testing.v1 import AppTest


@pytest.fixture
def gold_df():
    return pd.DataFrame(
        {
            "product_id": ["BTC-USD", "ETH-USD", "BTC-USD"],
            "minute_start": [
                "2026-10-14T12:00:00Z",
                "2026-10-14T12:00:00Z",
                "2026-10-14T12:01:00Z",
            ],
            "trade_count": [3, 1, 2],
            "notional_volume": [Decimal("400.00"), Decimal("80.00"), Decimal("120.00")],
            "vwap": [Decimal("100.00"), Decimal("20.00"), Decimal("120.00")],
        }
    )


def test_load_gold_data_reads_all_parquet_parts_and_preserves_decimals(tmp_path, gold_df):
    gold_path = tmp_path / "gold"
    gold_path.mkdir()
    gold_df.iloc[:2].to_parquet(gold_path / "part-00000.parquet", index=False)
    gold_df.iloc[2:].to_parquet(gold_path / "part-00001.parquet", index=False)
    (gold_path / "_SUCCESS").touch()
    (gold_path / ".part-00000.parquet.crc").write_bytes(b"metadata")

    loaded = app.load_gold_data(str(gold_path))

    assert len(loaded) == 3
    assert str(loaded["minute_start"].dt.tz) == "UTC"
    assert loaded["minute_start"].min() == pd.Timestamp("2026-10-14T12:00:00Z")
    assert loaded["notional_volume"].sum() == Decimal("600.00")
    assert all(isinstance(value, Decimal) for value in loaded["vwap"])


def test_load_gold_data_reports_missing_columns(tmp_path, gold_df):
    path = tmp_path / "gold.parquet"
    gold_df.drop(columns=["vwap", "trade_count"]).to_parquet(path, index=False)

    with pytest.raises(ValueError, match="trade_count, vwap"):
        app.load_gold_data(str(path))


def test_load_gold_data_reports_missing_dataset(tmp_path):
    with pytest.raises(FileNotFoundError, match="Gold Parquet dataset not found"):
        app.load_gold_data(str(tmp_path / "missing"))


@pytest.mark.parametrize("invalid_time", ["invalid-time", None])
def test_load_gold_data_rejects_invalid_timestamps(tmp_path, gold_df, invalid_time):
    path = tmp_path / "gold.parquet"
    gold_df.loc[0, "minute_start"] = invalid_time
    gold_df.to_parquet(path, index=False)

    with pytest.raises(ValueError):
        app.load_gold_data(str(path))


def test_dashboard_product_filter_updates_metric_and_table(tmp_path, monkeypatch, gold_df):
    monkeypatch.chdir(tmp_path)
    gold_path = tmp_path / app.DEFAULT_GOLD_PATH
    gold_path.mkdir(parents=True)
    gold_df.to_parquet(gold_path / "part-00000.parquet", index=False)

    page = AppTest.from_file(str(Path(app.__file__).resolve())).run()

    assert not page.exception
    assert page.title[0].value == "MarketGuard"
    assert page.metric[0].value == "6"
    assert page.selectbox[0].options == ["All products", "BTC-USD", "ETH-USD"]
    assert len(page.dataframe[0].value) == 3

    page.selectbox[0].select("BTC-USD").run()
    assert not page.exception
    assert page.metric[0].value == "5"
    assert set(page.dataframe[0].value["product_id"]) == {"BTC-USD"}

    page.selectbox[0].select("ETH-USD").run()
    assert not page.exception
    assert page.metric[0].value == "1"
    assert set(page.dataframe[0].value["product_id"]) == {"ETH-USD"}


def test_dashboard_displays_missing_input_as_page_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    page = AppTest.from_file(str(Path(app.__file__).resolve())).run()

    assert not page.exception
    assert "Gold Parquet dataset not found" in page.error[0].value
    assert not page.metric


def test_dashboard_displays_empty_dataset(tmp_path, monkeypatch, gold_df):
    monkeypatch.chdir(tmp_path)
    gold_path = tmp_path / app.DEFAULT_GOLD_PATH
    gold_path.mkdir(parents=True)
    gold_df.iloc[:0].to_parquet(gold_path / "part-00000.parquet", index=False)

    page = AppTest.from_file(str(Path(app.__file__).resolve())).run()

    assert not page.exception
    assert "empty" in page.info[0].value
    assert not page.metric
