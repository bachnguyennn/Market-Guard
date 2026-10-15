# MarketGuard

Repository: https://github.com/bachnguyennn/Market-Guard

## Run the finished local demo

Prerequisites: Python 3.11+, a Java 17+ runtime, Docker, and free local ports
9092 (Kafka) and 8502 (demo dashboard). From this repository directory:

```sh
python -m venv .venv
.venv/bin/python -m pip install -e .
./scripts/demo.sh
```

Open **http://127.0.0.1:8502** when the script prints `MarketGuard demo ready`.
The dashboard is automatically configured to read this run's actual outputs.
No manual sidebar path changes, Azure credentials, or cloud resources are
required. First startup may download the Kafka image and Spark JVM packages.

The script checks Docker, starts `marketguard-kafka` (or creates it using
`apache/kafka:4.3.1`), waits for broker metadata, and creates an isolated
two-partition demo topic. A separate Spark worker processes Kafka batches into
Silver Delta. The verifier waits for both the corresponding Spark checkpoint
completion and the expected Delta count, rather than using a fixed startup
sleep. It then checks these scenarios:

| Scenario | Expected Silver rows |
| --- | ---: |
| Publish 100 unique trades | 100 |
| Replay all 100; wait for their offsets to complete | 100 |
| Restart with the same checkpoint and replay all 100 again | 100 |

The deterministic dataset has 50 trades per pair across 15 consecutive minutes
(three or four trades per bucket), nonzero historical variance, a BTC volume
spike at 12:12 UTC, and an ETH price jump at 12:13 UTC. It produces 30 Gold
buckets with 100 total trades. The verifier checks every persisted trade and
every bucket's count, base volume, notional volume, and VWAP against an
independent `Decimal` reference, using an absolute `Decimal("0.000001")`
tolerance for Spark's decimal result scale. It also checks anomaly bucket
uniqueness, flag/type consistency, and no alerts during the initial warm-up.

The script verifies both intended signals, refreshes Gold/anomaly Parquet, and
starts Streamlit only after the files are readable. `summary.json`, `worker.log`,
`dashboard.log`, the Delta table, and the Parquet outputs are saved in
`data/streaming/final_demo_<id>/`.

Use Ctrl+C to stop the demo's Spark worker and Streamlit child processes. The
Kafka container and demo artifacts stay available for inspection. Each new run
uses a fresh topic/table/checkpoint; earlier experiments are retained.

```sh
./scripts/demo.sh --verify-only
./scripts/demo.sh --port 8503 --timeout 240
```

`--verify-only` runs the same ingestion/replay/anomaly checks and exits after
cleaning up its worker. `--timeout` bounds each Silver readiness check; broker
and dashboard health checks have their own bounded deadlines. `--run-dir` must
name a new directory. `MARKETGUARD_PYTHON` can select another installed Python
environment. On failure, inspect the run's logs rather than deleting only a
checkpoint or table.

### Separate publishing and verification commands

The three scripts have separate responsibilities: `demo.sh` starts and
orchestrates the local components, `generate_demo_trades.py` publishes the
deterministic events, and `verify_demo.py` checks an existing run without
publishing or modifying its outputs. The orchestrator reuses the same publisher
and verifier implementation as the standalone scripts.

After a run, copy its topic and directory from `summary.json`:

```sh
.venv/bin/python scripts/generate_demo_trades.py \
  --topic 'marketguard.demo.<id>' --receipt /tmp/marketguard-delivery.json
.venv/bin/python scripts/verify_demo.py \
  --run-dir 'data/streaming/final_demo_<id>'
```

Publishing again deliberately replays the same 100 keys. Keep the worker
running and let its checkpoint advance before rechecking. The publisher waits
for delivery callbacks and a bounded `flush()`, fails on delivery errors, and
returns acknowledged next offsets for every affected partition. The verifier
uses the run's recorded paths; it does not assume the default batch output
directory. Neither standalone command starts Kafka or a streaming worker.

### Architecture and deployment scope

```mermaid
flowchart LR
    Synthetic[Deterministic Coinbase-style events] --> Kafka[Local Kafka: two partitions]
    Kafka --> Spark[Spark Structured Streaming]
    Spark --> Delta[Delta Silver: insert-only MERGE]
    Delta --> Refresh[Finite Gold and anomaly refresh]
    Refresh --> Parquet[Gold and anomaly Parquet]
    Parquet --> Dashboard[Streamlit: metrics and alerts]
    Coinbase[Live Coinbase WebSocket] --> Collector[Existing Azure collector path]
    Collector --> Blob[Azure Blob Bronze archive]
```

The local Kafka/Spark demo and the existing Azure archive collector are distinct
execution paths. This command publishes synthetic events locally; it does not
start the live WebSocket collector, upload to Azure, or imply that Spark and the
dashboard are continuously cloud-hosted. The architecture can later connect the
WebSocket producer to Kafka, while retaining Blob Storage as the Bronze archive.

### Screenshots

Actual local demo outputs, with 100 trades and 15 minutes per pair:

![Market overview with a BTC volume spike](docs/screenshots/market-overview.png)

![Statistical volume and price alerts](docs/screenshots/anomaly-alerts.png)

To recreate these from a running demo:

```sh
.venv/bin/python -m pip install -e '.[screenshots]'
.venv/bin/python -m playwright install chromium
.venv/bin/python -m marketguard.dashboard.capture
```

Alternatively, pass `--browser-executable <path-to-Chrome>` to the capture
command to use an existing Chrome installation.

### Automated verification

```sh
.venv/bin/python -m pytest tests/test_local_demo.py tests/test_demo_verifier.py tests/test_dashboard.py tests/test_dashboard_data.py
MARKETGUARD_KAFKA_E2E=1 .venv/bin/python -m pytest tests/test_demo_e2e.py -v
.venv/bin/python -m pytest tests/test_kafka_to_delta.py -v
```

The opt-in end-to-end test invokes `scripts/demo.sh --verify-only` against real
Docker/Kafka and Spark/Delta, verifies replay counts and both anomaly scenarios,
then renders Streamlit with the generated files and checks BTC filtering.
The focused suite covers deterministic history, bounded readiness, partition
delivery receipts, corrupted aggregates, decimal tolerance, and dashboard
loading/filtering. GitHub Actions has a separate
`local-demo` job that runs this test. The smaller worker suite also verifies a
failure after Delta commit but before Spark checkpoint completion.

Local acceptance results (2026-10-14): **100 → 100 → 100 Silver trades**,
**30 Gold buckets**, and **two alerts** displayed in Streamlit. All aggregate
comparisons passed using Decimal arithmetic. The real end-to-end test also
verified product filtering: selecting BTC shows 50 trades and the BTC alert.
Validation passed: 40 focused tests, five worker/recovery tests, and one real
Kafka-to-dashboard integration test. These are local results; the workflow is
configured in GitHub Actions but its remote run status is not asserted here.
The tested environment used Python 3.12.12, PySpark 4.2.0, Delta Spark 4.4.1,
and Kafka 4.3.1; the Spark Kafka connector is matched to the installed PySpark
version automatically.

### Trade-offs and limitations

- One local Kafka broker and one writer per Delta table; this demo is not an HA deployment.
- Spark tracks Kafka offsets in its checkpoint. Separate Python consumers do not commit on its behalf.
- Insert-only MERGE provides replay tolerance for `(product_id, trade_id)`; conflicting existing trades are retained.
- Gold/anomaly/dashboard files are batch snapshots, not a continuously refreshed or globally atomic view.
- Late-event correction, within-minute ingestion-health checks, and incremental Gold updates remain future work.
- Gap-aware segments prevent historical windows crossing observed gaps; malformed/invalid records are skipped by this exercise's validation.
- Alerts are statistical indicators for investigation, not findings of fraud or manipulation.

The following sections document the individual lesson commands and batch tools.

## Kafka to Delta and dashboard demo

The local streaming path is:

```text
Synthetic events → Kafka (two partitions) → Spark foreachBatch → Silver Delta
                                                              ↓ batch refresh
                                    Streamlit ← Gold + anomaly Parquet
```

With the package installed and the local broker running, run the complete finite
demo from the repository directory:

```sh
docker start marketguard-kafka
.venv/bin/python -m marketguard.demo_kafka_to_delta
```

The demo creates a dedicated Kafka topic, a fresh Delta table, and a matching
checkpoint under a new `data/streaming/demo_<id>` directory. It publishes ten
unique trades, republishes them, restarts Spark with the same checkpoint, and
adds five new trades. It asserts Silver counts `10 → 10 → 10 → 15`, verifies
unique trade keys, and writes Gold/anomaly Parquet for the dashboard. The JSON
report (also saved as `summary.json` in the run directory) gives every path.
The synthetic data includes a deliberate BTC volume/price irregularity.

Launch the dashboard and set its sidebar Gold and Anomaly Parquet paths to the
`gold_path` and `anomaly_path` from that report:

```sh
.venv/bin/python -m streamlit run src/dashboard/app.py
```

Each demo uses a fresh topic and run directory; existing raw-topic messages,
tables, checkpoints, and batch dashboard outputs are kept separately. Demo
topics and files remain available for inspection. Use `--run-dir <new-path>` to
choose a new output directory and `--bootstrap-servers <host:port>` for another
reachable broker.

To run the continuous worker on the original raw-trades topic:

```sh
.venv/bin/python -m marketguard.spark.kafka_to_delta
```

It reads `marketguard.trades.raw` at `localhost:9092`, reuses the explicit JSON
schema, validates and deduplicates each batch, and inserts unmatched keys into
`data/streaming/silver_delta` every ten seconds. Progress belongs to
`data/checkpoints/kafka_to_delta`. It logs the batch ID and validated incoming
row count only after a successful MERGE; that count includes replayed trades,
not just newly inserted rows. Spark loads compatible Kafka and Delta packages.

Use `--available-now` to process the current backlog and exit. Custom `--topic`,
`--silver-path`, and `--checkpoint` options allow isolated experiments. Keep the
table/checkpoint pair stable between restarts; `earliest` only applies to fresh
queries. Spark owns Kafka source offsets through its checkpoint; this worker
does not use the Python consumers to commit offsets.

CI runs the following tests in a dedicated Spark process, using temporary
tables and file streams carrying Kafka-shaped values; no broker is required:

```sh
.venv/bin/python -m pytest tests/test_kafka_to_delta.py -v
```

They cover duplicate replay, invalid/empty records, write-failure propagation,
and the failure window where Delta has committed but Spark has not completed
its checkpoint. Restarting that failed query replays the batch without adding
trades. The local MVP assumes one writer per table and an existing correctly
configured broker. Gold and anomaly outputs refresh in batches; live Coinbase
production, ingestion-health monitoring, and cloud scheduling remain separate.

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
