# Bitcoin Fingerprint AI

This repository provides a small AI-flavored data pipeline that downloads Bitcoin (BTC-USD) prices from Yahoo Finance and generates "fingerprints" for several time horizons.  Each fingerprint summarizes how the market moved during a specific time window and can be used to build more advanced detectors, alerts, or similarity search features.

## Features
- Fetches up-to-date Bitcoin candles from Yahoo Finance using the [`yfinance`](https://pypi.org/project/yfinance/) API.
- Indexes every change in the closing price and stores the delta in the data frame.
- Saves every raw candle to a local SQLite database (via SQLAlchemy) for long-term indexing and reproducibility.
- Builds fingerprints for sliding windows of **1 minute, 5 minutes, 10 minutes, 30 minutes, 1 hour, 6 hours, 12 hours, 24 hours, and 7 days**.
- Persists the fingerprint catalog as JSON so it can be queried or imported into a vector database.

## Getting started

1. **Install dependencies**

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Run the fingerprint generator**

   ```bash
   python -m src.main --output output/fingerprints.json
   ```

   The script fetches the last seven days of one-minute BTC-USD candles (you can customize this with `--period` or by passing an explicit `--start-date`).  It emits a concise table to the console, saves the fingerprints and change index as JSON, and mirrors every raw candle into `data/bitcoin_prices.db`.

## Output structure
- `fingerprints`: list of fingerprints for every window and position.
- `price_changes`: list of price changes between consecutive candles (timestamp + delta).

See the generated JSON file for a ready-to-ingest artifact.
