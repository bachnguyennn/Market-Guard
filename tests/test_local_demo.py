import json
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from unittest.mock import Mock

import pytest
from marketguard.demo_data import synthetic_market_history
from marketguard.local_demo import committed_offsets, wait_for_silver


def test_demo_history_has_full_nonzero_baselines_and_unique_keys():
    trades = synthetic_market_history()
    assert len(trades) == 30
    assert len({(trade["product_id"], trade["trade_id"]) for trade in trades}) == 30
    for product in ["BTC-USD", "ETH-USD"]:
        rows = [trade for trade in trades if trade["product_id"] == product]
        times = [datetime.fromisoformat(trade["time"]) for trade in rows]
        assert len(times) == 15
        assert all(current - previous == timedelta(minutes=1) for previous, current in pairwise(times))
        baseline = [Decimal(row["price"]) * Decimal(row["size"]) for row in rows[7:12]]
        assert len(set(baseline)) > 1


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
