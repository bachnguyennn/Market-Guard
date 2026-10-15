import argparse
import json
from datetime import datetime
from decimal import Decimal, localcontext
from pathlib import Path

from .demo_data import synthetic_market_history

TOLERANCE = Decimal("0.000001")


def verify_gold_records(trades: list[dict], rows: list[dict]) -> None:
    """Compare every minute aggregate against an independent Decimal reference."""
    expected = {}
    with localcontext() as context:
        context.prec = 50
        for trade in trades:
            minute = int(datetime.fromisoformat(trade["time"]).timestamp()) // 60 * 60
            bucket = expected.setdefault(
                (trade["product_id"], minute),
                {"trade_count": 0, "base_volume": Decimal(0), "notional_volume": Decimal(0)},
            )
            bucket["trade_count"] += 1
            bucket["base_volume"] += Decimal(trade["size"])
            bucket["notional_volume"] += Decimal(trade["size"]) * Decimal(trade["price"])
        observed = {}
        for row in rows:
            key = (row["product_id"], row["minute_epoch"])
            if key in observed:
                raise ValueError(f"Duplicate Gold bucket: {key}")
            observed[key] = row
        if observed.keys() != expected.keys():
            raise ValueError("Gold minute/product buckets do not match the input trades")
        for key, reference in expected.items():
            row = observed[key]
            if row["trade_count"] != reference["trade_count"]:
                raise ValueError(f"Incorrect Gold trade_count for {key}")
            reference["vwap"] = reference["notional_volume"] / reference["base_volume"]
            for field in ["base_volume", "notional_volume", "vwap"]:
                value = row[field]
                if not isinstance(value, Decimal) or not value.is_finite() or abs(value - reference[field]) > TOLERANCE:
                    raise ValueError(f"Incorrect Gold {field} for {key}: {value}")


def verify_demo_outputs(spark, silver_path: str, gold_path: str, anomaly_path: str) -> dict:
    """Verify the bounded demo's persisted trades, aggregates, and alert rows."""
    trades = synthetic_market_history()
    expected = {(trade["product_id"], trade["trade_id"]): trade for trade in trades}
    silver_rows = spark.read.format("delta").load(silver_path).selectExpr(
        "product_id", "trade_id", "price", "size", "cast(time as long) as time_epoch",
    ).collect()
    observed = {(row.product_id, row.trade_id): row for row in silver_rows}
    if len(silver_rows) != len(expected) or observed.keys() != expected.keys():
        raise ValueError("Silver must contain exactly the 100 expected unique trade keys")
    for key, trade in expected.items():
        row = observed[key]
        if (
            row.price != Decimal(trade["price"])
            or row.size != Decimal(trade["size"])
            or row.time_epoch != int(datetime.fromisoformat(trade["time"]).timestamp())
        ):
            raise ValueError(f"Silver trade content differs from the published event: {key}")
    gold_rows = [row.asDict() for row in spark.read.parquet(gold_path).selectExpr(
        "*", "cast(minute_start as long) as minute_epoch",
    ).collect()]
    verify_gold_records(trades, gold_rows)
    anomaly_rows = spark.read.parquet(anomaly_path).selectExpr(
        "*", "cast(minute_start as long) as minute_epoch",
    ).collect()
    gold_keys = {(row["product_id"], row["minute_epoch"]) for row in gold_rows}
    anomaly_keys = {(row.product_id, row.minute_epoch) for row in anomaly_rows}
    if len(anomaly_rows) != len(gold_rows) or anomaly_keys != gold_keys:
        raise ValueError("Anomaly rows must preserve unique Gold minute/product buckets")
    first_minute = min(key[1] for key in gold_keys)
    for row in anomaly_rows:
        volume = row.is_volume_spike is True
        price = row.is_statistical_price_jump is True
        expected_type = "BOTH" if volume and price else "VOLUME" if volume else "PRICE" if price else "NONE"
        if row.is_anomaly != (volume or price) or row.anomaly_type != expected_type:
            raise ValueError("Anomaly classification disagrees with its detector flags")
        if row.minute_epoch < first_minute + 5 * 60 and row.is_anomaly:
            raise ValueError("Insufficient historical observations must not trigger an alert")
    btc_spike = any(
        row.product_id == "BTC-USD" and row.minute_epoch == first_minute + 12 * 60 and row.is_volume_spike
        for row in anomaly_rows
    )
    eth_jump = any(
        row.product_id == "ETH-USD" and row.minute_epoch == first_minute + 13 * 60 and row.is_statistical_price_jump
        for row in anomaly_rows
    )
    if not btc_spike or not eth_jump:
        raise ValueError("The expected BTC volume spike and ETH price jump must both be flagged")
    return {
        "unique_silver_count": len(silver_rows), "gold_count": len(gold_rows),
        "anomaly_count": len(anomaly_rows), "alert_count": sum(row.is_anomaly for row in anomaly_rows),
        "btc_volume_spike_verified": True, "eth_price_jump_verified": True,
        "gold_aggregates_verified": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify an existing MarketGuard demo run without publishing events.")
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    summary = json.loads((args.run_dir / "summary.json").read_text(encoding="utf-8"))
    from .spark.kafka_to_delta import create_spark_session

    spark = create_spark_session("MarketGuard-VerifyExistingDemo", "local[1]")
    try:
        spark.conf.set("spark.databricks.delta.snapshotPartitions", "1")
        result = verify_demo_outputs(spark, summary["silver_path"], summary["gold_path"], summary["anomaly_path"])
        print(json.dumps(result, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
