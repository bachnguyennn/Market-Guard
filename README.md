# MarketGuard

Repository: https://github.com/bachnguyennn/Market-Guard

## Finite batch pipeline

From the repository directory, install the package into the project environment:

```sh
.venv/bin/python -m pip install -e .
.venv/bin/python -m marketguard.run_pipeline
```

With that environment activated, the entry point is:

```sh
python -m marketguard.run_pipeline
```

The command reads local Coinbase JSONL from `data/spark_input`, runs trade
validation and deduplication, builds one-minute Gold aggregates, and applies
gap-aware volume and statistical price detectors in UTC. It writes finite
Parquet snapshots to `data/batch_output/silver`, `data/batch_output/gold`, and
`data/batch_output/anomalies`, then reads every stage back to confirm its files
and row counts. It also checks Gold trade totals and anomaly-row preservation.
A JSON summary reports counts, alerts, and absolute output paths. Spark stops
and the command exits once processing finishes; failures propagate as errors.

Use different local input and output paths with:

```sh
python -m marketguard.run_pipeline --bronze data/raw --output-dir data/review_batch
```

Each run overwrites those three output snapshots. Input and output paths must
not overlap. Missing input or an input with no valid trades fails explicitly.
The default paths keep these batch snapshots separate from existing streaming
Delta tables and checkpoints. Each stage is verified separately; these writes
are not a single transaction across all three outputs.

The existing `src` tree is installed as the `marketguard` Python package, so the
lesson modules remain in place. The local Kafka console stream, incremental
Delta stream, and dashboard are separate entry points below; this finite command
reads local Bronze files and does not start collection or upload to Azure.

```sh
.venv/bin/python -m pytest tests/test_run_pipeline.py -v
```

## Gold dashboard

Generate the batch outputs, then launch the Streamlit MVP from the repository
directory:

```sh
.venv/bin/python -m pip install -e .
.venv/bin/python -m marketguard.run_pipeline
.venv/bin/python -m streamlit run src/dashboard/app.py
```

Open the local URL printed by Streamlit. The page reads the existing Parquet
dataset at `data/batch_output/gold` with pandas and PyArrow; it needs no running
Spark session, database, or API. Change the Gold path in the sidebar to inspect
another batch. The product selector filters total trade counts, the notional
volume chart, and the detail table in the Market Overview tab. All minute buckets
are displayed in UTC.

`load_gold_data()` validates required columns and timestamps and preserves
financial Decimal values. Only the chart copy is converted to numeric plotting
values. Missing, invalid, or empty datasets are explained on the page. This MVP
shows saved batch outputs; it does not automatically collect new trades.

The Anomaly Alerts tab reads `data/batch_output/anomalies`, with a separate path
field in the sidebar and the same product selector. It counts only rows where
`is_anomaly` is true, sorts alerts newest first, and shows available signal scores
and an explanation for each alert. A dataset without the combined flag can use
`is_volume_spike` and `is_statistical_price_jump` instead. Missing Boolean values
do not count as alerts, and string flags are rejected.

Two anomaly-output rows do not mean two alerts: the current saved sample has zero
flagged rows and displays "No anomalies detected in the selected period."
Alerts describe statistical irregularities for investigation. Missing or invalid
anomaly data is reported inside its tab while the market overview stays usable.
Parquet loading and pure product/alert filtering live in
`src/dashboard/data_loader.py`.

The existing `src` directory installs as `marketguard`, so the dashboard source
path here is `src/dashboard/app.py`.

```sh
.venv/bin/python -m pytest tests/test_dashboard.py tests/test_dashboard_data.py -v
```

## Local file streaming

Run from the `marketguard` directory using the project environment:

```sh
.venv/bin/python -m src.spark.streaming_pipeline
```

The driver creates `data/stream_input`, watches for new JSONL files, validates
Coinbase trades, and appends them to `data/silver_stream_delta`. Progress is saved
in `data/checkpoints/silver_stream`. Spark uses UTC and the Delta extensions.
Stop the driver with Ctrl+C; restart with the same table and checkpoint paths.

In another terminal, publish one complete local file at a time:

```sh
mkdir -p data/stream_input
printf '%s\n' '{"type":"match","product_id":"BTC-USD","trade_id":1001,"price":"68000.00","size":"0.15","time":"2026-10-14T12:00:00Z"}' > data/batch_001.jsonl
mv data/batch_001.jsonl data/stream_input/batch_001.jsonl
```

Use a new filename for each batch and finish writing it before moving it into
the watched directory. Editing an already discovered file does not create a new
input batch. Keep the checkpoint with its corresponding Delta table.

This exercise validates trades without streaming deduplication state. Duplicate
trade keys in distinct input records are preserved. The incremental driver below
uses Delta MERGE to deduplicate by `(product_id, trade_id)`.
The direct Delta streaming sink uses the checkpoint and transaction log to avoid
writing a retried micro-batch twice; this does not deduplicate separate events.

Run the local integration test with:

```sh
.venv/bin/python -m pytest tests/test_streaming_pipeline.py -v
```

## Incremental Silver with MERGE

```sh
.venv/bin/python -m src.spark.incremental_stream
```

This driver reads raw JSONL files from `data/incremental_stream_input` with the
explicit Coinbase schema. Every ten seconds, `foreachBatch` validates and
deduplicates the micro-batch, skips empty results, and merges new trade keys into
`data/silver_delta`. Existing matching trades remain unchanged. The table is
created by the first nonempty valid batch.

Move complete files with unique names into this input directory. Keep
`data/checkpoints/incremental_silver` and the Delta table together across restarts.
For a fresh experiment use separate input, table, and checkpoint paths together.

Write failures propagate and fail the query. Restarting with the same checkpoint
allows Spark to retry; replaying a committed batch is safe because MERGE ignores
existing `(product_id, trade_id)` keys. This file source tracks discovered files
in Spark's checkpoint and uses no Python Kafka consumer or offset commits.

```sh
.venv/bin/python -m pytest tests/test_incremental_stream.py -v -s
```

The tests verify Silver counts `0 → 2 → 3 → 3 → 4`, replay of the same batch ID,
empty/invalid batches, and propagation of Delta failures.

## Kafka streaming to the console

From the `marketguard` directory:

```sh
docker start marketguard-kafka
.venv/bin/python -m src.spark.kafka_stream
```

The driver loads the Kafka connector matching the installed PySpark version.
The verified local combination is Spark 4.2.0, Scala 2.13.18, and
`org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`. First startup may download JVM
dependencies. Kafka must advertise a broker reachable at `localhost:9092`.

Publish the synthetic trade in another terminal:

```sh
.venv/bin/python -m src.kafka.producer
```

Spark reads `marketguard.trades.raw`, decodes binary message values as JSON, and
prints `type`, `product_id`, `trade_id`, `price`, `size`, and `time`. All fields
remain strings until Silver transformation. Missing or malformed JSON fields
appear as null values in this parsing exercise.

Progress is stored in `data/checkpoints/kafka_console`, separately from Delta
checkpoints. Stop with Ctrl+C and restart with the same checkpoint to resume.
`startingOffsets=earliest` applies only to a fresh query. Spark tracks its own
source offsets; no Python consumer commits offsets for this query.

For a bounded restart experiment, run the following, publish another synthetic
trade, then run the same command again:

```sh
.venv/bin/python -m src.spark.kafka_stream --available-now
```

Each invocation consumes available data and exits. The second invocation resumes
from the same checkpoint and displays only newly published records. Use
`--checkpoint data/checkpoints/kafka_console_experiment` for a separate experiment.
The console sink is for inspection; it does not make console output transactional.

```sh
.venv/bin/python -m pytest tests/test_kafka_stream.py -v
```
