import argparse
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen
from uuid import uuid4

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import functions as F

from .anomoly_pipeline import build_anomaly_table
from .demo_data import synthetic_market_history
from .demo_kafka_to_delta import publish_trades
from .run_pipeline import write_and_verify
from .spark.bronze_to_silver import calculate_market_metrics
from .spark.kafka_to_delta import create_spark_session

LOGGER = logging.getLogger(__name__)


def committed_offsets(checkpoint: Path, topic: str) -> dict[int, int]:
    """Read offsets only for a batch whose Spark completion marker exists."""
    batches = [int(path.name) for path in (checkpoint / "commits").glob("*") if path.name.isdigit()]
    if not batches:
        return {}
    offset_file = checkpoint / "offsets" / str(max(batches))
    source_offsets = json.loads(offset_file.read_text().splitlines()[-1])
    return {int(partition): offset for partition, offset in source_offsets.get(topic, {}).items()}


def wait_for_silver(
    spark, worker, silver_path: Path, checkpoint: Path, topic: str,
    expected_offsets: dict[int, int], expected_count: int, timeout: float = 180,
) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if worker.poll() is not None:
            raise RuntimeError("Spark worker exited; inspect worker.log")
        completed = committed_offsets(checkpoint, topic)
        if all(completed.get(partition, -1) >= offset for partition, offset in expected_offsets.items()):
            count = spark.read.format("delta").load(str(silver_path)).count()
            if count != expected_count:
                raise RuntimeError(f"Expected {expected_count} Silver rows, observed {count}")
            return count
        time.sleep(min(0.5, max(0, deadline - time.monotonic())))
    raise TimeoutError(f"Silver readiness timed out: expected {expected_count} rows and committed offsets")


def stop_process(process) -> None:
    if process is None or process.poll() is not None:
        return
    for shutdown_signal, timeout in [(signal.SIGINT, 20), (signal.SIGTERM, 10), (signal.SIGKILL, 5)]:
        try:
            os.killpg(process.pid, shutdown_signal)
            process.wait(timeout=timeout)
            return
        except subprocess.TimeoutExpired:
            continue
        except ProcessLookupError:
            return


def wait_for_broker(admin: AdminClient, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if admin.list_topics(timeout=min(2, max(0.1, deadline - time.monotonic()))).brokers:
                return
        except KafkaException as error:
            LOGGER.debug("Kafka is not ready: %s", error)
        time.sleep(min(0.5, max(0, deadline - time.monotonic())))
    raise TimeoutError("Kafka did not become ready within the broker deadline")


def wait_for_dashboard(process, url: str, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Streamlit exited; inspect dashboard.log")
        try:
            with urlopen(f"{url}/_stcore/health", timeout=1) as response:
                if response.read() == b"ok":
                    return
        except (URLError, TimeoutError):
            pass
        time.sleep(min(0.25, max(0, deadline - time.monotonic())))
    raise TimeoutError("Streamlit did not become healthy within 30 seconds")


def run_demo(args) -> dict:
    run_id = uuid4().hex
    run_dir = Path(args.run_dir or f"data/streaming/final_demo_{run_id[:8]}").resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    topic = f"marketguard.demo.{run_id}"
    silver_path = run_dir / "silver_delta"
    checkpoint = run_dir / "checkpoint"
    broker_config = {"bootstrap.servers": args.bootstrap_servers, "broker.address.family": "v4"}
    admin = AdminClient(broker_config)
    wait_for_broker(admin)
    admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])[topic].result(timeout=30)
    metadata = admin.list_topics(topic, timeout=10).topics[topic]
    if metadata.error is not None or len(metadata.partitions) != 2:
        raise RuntimeError("Demo topic did not become ready with two partitions")
    producer = Producer({**broker_config, "client.id": "marketguard-final-demo"})
    trades = synthetic_market_history()
    worker = None
    dashboard = None
    spark = None
    worker_pids = []
    with (
        (run_dir / "worker.log").open("a", encoding="utf-8") as worker_log,
        (run_dir / "dashboard.log").open("a", encoding="utf-8") as dashboard_log,
    ):
        def start_worker():
            process = subprocess.Popen(
                [sys.executable, "-m", "marketguard.spark.kafka_to_delta",
                 "--topic", topic, "--bootstrap-servers", args.bootstrap_servers,
                 "--silver-path", str(silver_path), "--checkpoint", str(checkpoint),
                 "--master", "local[2]", "--processing-time", "1 second"],
                stdout=worker_log, stderr=subprocess.STDOUT, start_new_session=True,
            )
            worker_pids.append(process.pid)
            return process

        try:
            worker = start_worker()
            spark = create_spark_session("MarketGuard-DemoVerifier", "local[1]")
            spark.conf.set("spark.databricks.delta.snapshotPartitions", "1")
            counts = []
            expected_offsets = publish_trades(producer, topic, trades[:10])
            counts.append(wait_for_silver(spark, worker, silver_path, checkpoint, topic, expected_offsets, 10, args.timeout))
            print("Scenario A: 10 unique trades persisted", flush=True)
            expected_offsets.update(publish_trades(producer, topic, trades[:10]))
            counts.append(wait_for_silver(spark, worker, silver_path, checkpoint, topic, expected_offsets, 10, args.timeout))
            print("Scenario B: replay completed; Silver still has 10 unique trades", flush=True)
            stop_process(worker)
            worker = start_worker()
            counts.append(wait_for_silver(spark, worker, silver_path, checkpoint, topic, expected_offsets, 10, args.timeout))
            expected_offsets.update(publish_trades(producer, topic, trades[10:]))
            counts.append(wait_for_silver(spark, worker, silver_path, checkpoint, topic, expected_offsets, 30, args.timeout))

            silver_df = spark.read.format("delta").load(str(silver_path))
            unique_count = silver_df.select("product_id", "trade_id").distinct().count()
            if unique_count != 30:
                raise RuntimeError("Silver keys are not unique")
            gold_path = run_dir / "dashboard" / "gold"
            anomaly_path = run_dir / "dashboard" / "anomalies"
            gold_df, gold_count = write_and_verify(calculate_market_metrics(silver_df), gold_path)
            anomalies_df, anomaly_count = write_and_verify(build_anomaly_table(gold_df), anomaly_path)
            btc_spike = anomalies_df.filter(
                (F.col("product_id") == "BTC-USD")
                & (F.col("minute_start") == F.to_timestamp(F.lit("2026-10-14T12:12:00Z")))
                & F.col("is_volume_spike")
            ).count()
            eth_jump = anomalies_df.filter(
                (F.col("product_id") == "ETH-USD")
                & (F.col("minute_start") == F.to_timestamp(F.lit("2026-10-14T12:13:00Z")))
                & F.col("is_statistical_price_jump")
            ).count()
            if gold_count != 30 or anomaly_count != 30 or not btc_spike or not eth_jump:
                raise RuntimeError("The complete-history anomaly demonstration failed")
            summary = {
                "topic": topic, "partition_count": 2, "silver_counts": counts,
                "unique_silver_count": unique_count, "minutes_per_product": 15,
                "silver_path": str(silver_path), "checkpoint_path": str(checkpoint),
                "gold_path": str(gold_path), "anomaly_path": str(anomaly_path),
                "gold_count": gold_count, "anomaly_count": anomaly_count,
                "alert_count": anomalies_df.filter("is_anomaly").count(),
                "btc_volume_spike_verified": bool(btc_spike),
                "eth_price_jump_verified": bool(eth_jump), "worker_pids": worker_pids,
            }
            summary_path = run_dir / "summary.json"
            summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
            print("Scenario C: 15 consecutive minutes per pair; BTC volume and ETH price alerts verified", flush=True)
            print(json.dumps(summary, indent=2), flush=True)
            spark.stop()
            spark = None
            if args.verify_only:
                print("MarketGuard demo verified successfully.", flush=True)
                return summary

            with socket.socket() as probe:
                probe.bind(("127.0.0.1", args.port))
            environment = {
                **os.environ, "MARKETGUARD_GOLD_PATH": str(gold_path),
                "MARKETGUARD_ANOMALY_PATH": str(anomaly_path),
                "MARKETGUARD_DEMO_SUMMARY": str(summary_path),
            }
            dashboard = subprocess.Popen(
                [sys.executable, "-m", "streamlit", "run", str(Path(__file__).parent / "dashboard" / "app.py"),
                 "--server.headless=true", "--server.address=127.0.0.1", f"--server.port={args.port}",
                 "--browser.gatherUsageStats=false"],
                env=environment, stdout=dashboard_log, stderr=subprocess.STDOUT, start_new_session=True,
            )
            url = f"http://127.0.0.1:{args.port}"
            wait_for_dashboard(dashboard, url)
            print(f"MarketGuard demo ready: {url}\nReport: {summary_path}\nPress Ctrl+C to stop demo processes.", flush=True)
            return_code = dashboard.wait()
            if return_code:
                raise RuntimeError(f"Streamlit exited with status {return_code}")
            return summary
        finally:
            try:
                if spark is not None:
                    spark.stop()
            finally:
                stop_process(dashboard)
                stop_process(worker)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the complete local MarketGuard demo.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--run-dir")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--port", type=int, default=8502)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    def shutdown(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, shutdown)
    try:
        run_demo(args)
    except KeyboardInterrupt:
        print("Stopping MarketGuard demo processes.", flush=True)


if __name__ == "__main__":
    main()
