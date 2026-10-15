import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest
from marketguard.dashboard import app
from streamlit.testing.v1 import AppTest


@pytest.mark.skipif(os.getenv("MARKETGUARD_KAFKA_E2E") != "1", reason="Opt-in test requires local Docker and Kafka")
def test_real_kafka_to_dashboard_demo(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[1]
    run_dir = Path(os.getenv("MARKETGUARD_DEMO_RUN_DIR", str(tmp_path / "demo"))).resolve()
    result = subprocess.run(
        ["bash", str(root / "scripts" / "demo.sh"), "--verify-only", "--run-dir", str(run_dir)],
        cwd=root, env={**os.environ, "MARKETGUARD_PYTHON": sys.executable},
        capture_output=True, text=True, timeout=420, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    summary = json.loads((run_dir / "summary.json").read_text())
    assert summary["silver_counts"] == [100, 100, 100]
    assert summary["unique_silver_count"] == 100
    assert summary["gold_aggregates_verified"]
    assert summary["partition_count"] == 2
    assert len(set(summary["worker_pids"])) == 2
    assert summary["btc_volume_spike_verified"]
    assert summary["eth_price_jump_verified"]
    gold = pd.read_parquet(summary["gold_path"])
    assert len(gold) == 30
    assert gold.groupby("product_id").size().to_dict() == {"BTC-USD": 15, "ETH-USD": 15}
    assert int(gold["trade_count"].sum()) == 100

    monkeypatch.setenv("MARKETGUARD_GOLD_PATH", summary["gold_path"])
    monkeypatch.setenv("MARKETGUARD_ANOMALY_PATH", summary["anomaly_path"])
    page = AppTest.from_file(app.__file__, default_timeout=30).run()
    assert not page.exception
    assert page.tabs[0].metric[0].value == "100"
    assert int(page.tabs[1].metric[0].value) == summary["alert_count"] >= 2
    alerts = page.tabs[1].dataframe[0].value
    assert alerts["is_anomaly"].all()
    assert alerts["explanation"].notna().all()
    page.selectbox[0].select("BTC-USD").run()
    assert not page.exception
    assert page.tabs[0].metric[0].value == "50"
    assert set(page.tabs[1].dataframe[0].value["product_id"]) == {"BTC-USD"}
