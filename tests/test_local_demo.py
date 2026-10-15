import json
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from marketguard import local_demo
from marketguard.demo_data import synthetic_market_history
from marketguard.local_demo import committed_offsets, wait_for_silver


def test_demo_history_has_full_nonzero_baselines_and_unique_keys():
    trades = synthetic_market_history()
    assert len(trades) == 100
    assert len({(trade["product_id"], trade["trade_id"]) for trade in trades}) == 100
    assert trades == synthetic_market_history()
    for product in ["BTC-USD", "ETH-USD"]:
        rows = [trade for trade in trades if trade["product_id"] == product]
        assert len(rows) == 50
        times = sorted({datetime.fromisoformat(trade["time"]).replace(second=0) for trade in rows})
        assert len(times) == 15
        assert all(current - previous == timedelta(minutes=1) for previous, current in pairwise(times))
        volumes = defaultdict(Decimal)
        for row in rows:
            minute = datetime.fromisoformat(row["time"]).minute
            volumes[minute] += Decimal(row["size"])
        assert len({volumes[minute] for minute in range(7, 12)}) > 1
        assert volumes[12] == (Decimal(25) if product == "BTC-USD" else Decimal("0.9"))


def test_checkpoint_readiness_ignores_uncommitted_offsets(tmp_path):
    (tmp_path / "commits").mkdir()
    (tmp_path / "offsets").mkdir()
    for batch, offset in [(0, 5), (1, 10)]:
        (tmp_path / "offsets" / str(batch)).write_text(
            "v1\n{}\n" + json.dumps({"trades": {"0": offset, "1": offset}})
        )
    (tmp_path / "commits" / "0").write_text("v1\n{}")

    assert committed_offsets(tmp_path, "trades") == {0: 5, 1: 5}
    (tmp_path / "commits" / "1").write_text("v1\n{}")
    assert committed_offsets(tmp_path, "trades") == {0: 10, 1: 10}


def test_silver_readiness_is_bounded(tmp_path):
    worker = Mock()
    worker.poll.return_value = None
    with pytest.raises(TimeoutError, match="readiness timed out"):
        wait_for_silver(Mock(), worker, tmp_path / "silver", tmp_path, "trades", {0: 10}, 10, timeout=0.01)


def test_silver_readiness_reports_dead_worker(tmp_path):
    worker = Mock()
    worker.poll.return_value = 1
    with pytest.raises(RuntimeError, match="worker exited"):
        wait_for_silver(Mock(), worker, tmp_path / "silver", tmp_path, "trades", {0: 10}, 10)


@pytest.mark.parametrize("verifier_fails", [False, True])
def test_demo_resolves_verifier_dependencies_before_starting_worker(tmp_path, monkeypatch, verifier_fails):
    admin = Mock()
    admin.create_topics.return_value = {"marketguard.demo.topic": Mock()}
    monkeypatch.setattr(local_demo, "uuid4", lambda: SimpleNamespace(hex="topic"))
    admin.list_topics.return_value.topics = {
        "marketguard.demo.topic": SimpleNamespace(error=None, partitions={0: None, 1: None})
    }
    monkeypatch.setattr(local_demo, "AdminClient", Mock(return_value=admin))
    monkeypatch.setattr(local_demo, "wait_for_broker", Mock())
    monkeypatch.setattr(local_demo, "Producer", Mock())
    initialized = []
    session = Mock()

    def initialize_verifier(*args):
        if verifier_fails:
            raise RuntimeError("verifier startup failed")
        initialized.append(True)
        return session

    def start_worker(*args, **kwargs):
        assert initialized, "Worker must wait until verifier dependency resolution finishes"
        return Mock(pid=123)

    worker_start = Mock(side_effect=start_worker)
    monkeypatch.setattr(local_demo, "create_spark_session", initialize_verifier)
    monkeypatch.setattr(local_demo.subprocess, "Popen", worker_start)
    monkeypatch.setattr(local_demo, "stop_process", Mock())
    monkeypatch.setattr(local_demo, "publish_trades", Mock(side_effect=RuntimeError("stop after startup")))
    args = SimpleNamespace(run_dir=str(tmp_path / "demo"), bootstrap_servers="localhost:9092")

    with pytest.raises(RuntimeError, match="verifier startup failed" if verifier_fails else "stop after startup"):
        local_demo.run_demo(args)

    assert worker_start.call_count == (0 if verifier_fails else 1)
    assert session.stop.call_count == (0 if verifier_fails else 1)
