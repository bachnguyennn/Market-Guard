from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest
from marketguard.dashboard import app
from marketguard.dashboard.data_loader import (
    load_anomaly_data,
    load_gold_data,
    select_alerts,
)
from streamlit.testing.v1 import AppTest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_public_snapshot_is_small_synthetic_and_has_expected_metrics_and_alerts():
    gold = load_gold_data(str(PROJECT_ROOT / "data/demo/gold.parquet"))
    anomalies = load_anomaly_data(str(PROJECT_ROOT / "data/demo/anomalies.parquet"))
    assert len(gold) == len(anomalies) == 30
    assert int(gold["trade_count"].sum()) == 100
    assert gold.groupby("product_id").size().to_dict() == {"BTC-USD": 15, "ETH-USD": 15}
    assert not gold.duplicated(["product_id", "minute_start"]).any()
    assert not anomalies.duplicated(["product_id", "minute_start"]).any()
    assert all(isinstance(value, Decimal) for value in gold["vwap"])
    alerts = select_alerts(anomalies)
    assert len(alerts) == 2
    assert set(zip(alerts["product_id"], alerts["anomaly_type"], strict=True)) == {("BTC-USD", "VOLUME"), ("ETH-USD", "PRICE")}
    assert gold["minute_start"].min() == pd.Timestamp("2026-10-14T12:00:00Z")
    for path in (PROJECT_ROOT / "data/demo").glob("*.parquet"):
        assert path.stat().st_size < 100_000


@pytest.mark.parametrize("entrypoint", ["streamlit_app.py", "deploy/streamlit_app.py"])
def test_public_entrypoints_work_outside_project_and_ignore_local_path_settings(tmp_path, monkeypatch, entrypoint):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MARKETGUARD_GOLD_PATH", "/private/local/gold")
    monkeypatch.setenv("MARKETGUARD_ANOMALY_PATH", "/private/local/anomalies")
    page = AppTest.from_file(str(PROJECT_ROOT / entrypoint), default_timeout=30).run()
    assert not page.exception
    assert not page.error
    assert not page.sidebar.text_input
    assert "Synthetic demo snapshot" in page.info[0].value
    assert "not a live market feed" in page.info[0].value
    assert page.tabs[0].metric[0].value == "100"
    assert page.tabs[1].metric[0].value == "2"
    for product, alert_type in [("BTC-USD", "VOLUME"), ("ETH-USD", "PRICE")]:
        page.selectbox[0].select(product).run()
        assert not page.exception
        assert page.tabs[0].metric[0].value == "50"
        assert page.tabs[1].metric[0].value == "1"
        assert page.tabs[1].dataframe[0].value["anomaly_type"].tolist() == [alert_type]
        assert set(page.tabs[0].dataframe[0].value["product_id"]) == {product}


def test_public_missing_data_error_does_not_expose_server_paths(tmp_path, monkeypatch):
    private_path = tmp_path / "private-server-path"
    monkeypatch.setattr(app, "DEMO_DATA_PATH", private_path)
    page = AppTest.from_file(str(PROJECT_ROOT / "streamlit_app.py")).run()
    assert not page.exception
    assert page.error[0].value == "The bundled demo market data is unavailable."
    assert str(private_path) not in page.error[0].value


def test_public_alert_error_does_not_expose_server_paths(tmp_path, monkeypatch):
    gold = load_gold_data(str(PROJECT_ROOT / "data/demo/gold.parquet"))
    gold.to_parquet(tmp_path / "gold.parquet", index=False)
    monkeypatch.setattr(app, "DEMO_DATA_PATH", tmp_path)
    page = AppTest.from_file(str(PROJECT_ROOT / "streamlit_app.py")).run()
    assert not page.exception
    assert page.tabs[0].metric[0].value == "100"
    assert page.tabs[1].error[0].value == "The bundled demo alerts are unavailable."


def test_cloud_requirements_only_include_dashboard_libraries():
    requirements = (PROJECT_ROOT / "deploy/requirements.txt").read_text().splitlines()
    assert {line.split("==")[0] for line in requirements} == {"streamlit", "pandas", "pyarrow", "azure-storage-blob"}
    assert (PROJECT_ROOT / "requirements-dashboard.txt").read_text().strip() == "-r deploy/requirements.txt"
