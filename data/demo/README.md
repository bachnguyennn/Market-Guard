# Synthetic public dashboard snapshot

These two compact Parquet files are safe, precomputed outputs, not real market
observations or live prices. They contain 100 generated trades summarized into
15 consecutive UTC minutes for each of BTC-USD and ETH-USD (30 Gold rows).

The source generator deliberately includes a BTC volume spike at 12:12 UTC and
an ETH price jump at 12:13 UTC on 2026-10-14. The anomaly table contains 30
evaluated rows, of which two are flagged. Alerts are statistical irregularities,
not confirmed fraud or investment recommendations.

Regenerate from the project root with the full local pipeline environment:

```sh
.venv/bin/python scripts/build_dashboard_demo.py
```

This command uses the existing deterministic generator and Spark aggregation
and detector functions. It does not read raw Bronze data, contact Coinbase,
Kafka, or Azure, or need cloud credentials. Only these safe outputs are
allowlisted in Git; all other generated data remains ignored.
