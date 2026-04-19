# Binance USDT-M Perpetual Futures — Historical 1m OHLCV Downloader

Downloads historical 1-minute candlestick data for Binance USDT-M Perpetual Futures and saves it as Parquet files.

> ⚠️ This downloads **USDT-M Perpetual Futures** data, not spot prices. Prices are similar but not identical. If you need spot data, change the exchange in `download.py` to `ccxt.binance()` and update the symbol format in `config.py` (e.g. `"BTC/USDT"` instead of `"BTC/USDT:USDT"`).

**Default config:** 6 pairs (BTC, ETH, SOL, BNB, XRP, DOGE), 2021-01-01 → 2026-04-01, ~2.76M rows per file.

**Download time:** ~30–60 min for all 6 pairs over 5 years, depending on connection speed and Binance rate limits.

## Stack

- [ccxt](https://github.com/ccxt/ccxt) — exchange API
- [pandas](https://pandas.pydata.org/) — data wrangling
- [pyarrow](https://arrow.apache.org/docs/python/) — Parquet write (zstd compression)

## Setup

```bash
pip install -r requirements.txt
```

## Configuration

Edit `config.py` before running:

```python
SYMBOLS = [
    "BTC/USDT:USDT",   # ccxt notation for USDT-M perpetual futures on Binance
    "ETH/USDT:USDT",   # format: "{BASE}/{QUOTE}:{SETTLE}"
    ...
]
TIMEFRAME  = "1m"
START_DATE = "2021-01-01T00:00:00Z"
END_DATE   = "2026-04-01T00:00:00Z"
DATA_DIR   = "data"
```

Both scripts read from this file — change once, applies everywhere.

> **Note:** `check.py` calculates coverage using `START_DATE` and `END_DATE` from `config.py`. If you change these values after downloading, re-run `download.py` to keep the data consistent with the config.

## Usage

**Download:**
```bash
python download.py
```

Downloads sequentially, logs progress every 50 batches (~1.25% of the range per pair). Writes to a `.tmp` file first, renames atomically on completion. Re-running overwrites existing files from scratch — incremental updates are not supported.

**Validate:**
```bash
python check.py
```

Checks every `.parquet` file in `DATA_DIR` for: row count and coverage, duplicates, sort order, gaps > 5 min, OHLC constraint violations, correct dtypes.

## Output format

Each file: `{SYMBOL}_{TIMEFRAME}.parquet` (e.g. `BTCUSDT_1m.parquet`)

| Column | Type |
|---|---|
| `timestamp` | `datetime64[ns, UTC]` |
| `open` | `float64` |
| `high` | `float64` |
| `low` | `float64` |
| `close` | `float64` |
| `volume` | `float64` |

No index, sorted ascending by timestamp, no duplicates.

The output is a standard pandas-compatible Parquet file. Works out of the box with VectorBT, Backtrader, backtesting.py, or any tool that reads a pandas DataFrame.

## Gotchas

- **Maintenance windows** — Binance fills exchange maintenance periods with synthetic candles (`volume=0`) rather than leaving gaps. Expect ~180 such minutes per pair over 5 years. Filter with `volume > 0` if your strategy is sensitive to liquidity.
- **Late-listed pairs** — if a pair was listed after `START_DATE`, `check.py` reports coverage against the actual listing date instead.
- **No incremental update** — re-running `download.py` re-downloads the full range from scratch. If you only need to extend the date range, update `END_DATE` in `config.py` and re-run.
- **DuckDB tip** — when querying with DuckDB, timestamps display in the session local timezone by default. To see UTC: `SET TimeZone = 'UTC';`
