import os
import time
import ccxt
import pandas as pd

from config import SYMBOLS, TIMEFRAME, START_DATE, END_DATE, DATA_DIR

BATCH_LIMIT = 1500
MAX_RETRIES = 5
MINUTE_MS   = 60_000


def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def symbol_to_filename(symbol):
    return symbol.replace("/", "").replace(":USDT", "") + f"_{TIMEFRAME}.parquet"


def fetch_with_retry(exchange, symbol, since_ms, limit):
    transient = (
        ccxt.NetworkError,
        ccxt.RequestTimeout,
        ccxt.RateLimitExceeded,
        ccxt.ExchangeNotAvailable,
    )
    for attempt in range(MAX_RETRIES):
        try:
            return exchange.fetch_ohlcv(symbol, TIMEFRAME, since=since_ms, limit=limit)
        except transient as e:
            if attempt == MAX_RETRIES - 1:
                raise
            delay = 2 ** attempt
            log(f"  transient error ({type(e).__name__}): {e} — retry in {delay}s")
            time.sleep(delay)


def download_symbol(exchange, symbol):
    filename = symbol_to_filename(symbol)
    path = os.path.join(DATA_DIR, filename)
    tmp_path = path + ".tmp"

    since_ms = int(pd.Timestamp(START_DATE).timestamp() * 1000)
    end_ms = int(pd.Timestamp(END_DATE).timestamp() * 1000)
    total_range = end_ms - since_ms

    log(f"[{symbol}] start: {pd.to_datetime(since_ms, unit='ms', utc=True)} → "
        f"{pd.to_datetime(end_ms, unit='ms', utc=True)}")

    rows = []
    cursor = since_ms
    batch_n = 0
    while cursor < end_ms:
        batch = fetch_with_retry(exchange, symbol, cursor, BATCH_LIMIT)
        batch_n += 1
        if not batch:
            log(f"[{symbol}] empty batch at {pd.to_datetime(cursor, unit='ms', utc=True)} — stop")
            break

        kept = [c for c in batch if c[0] < end_ms]
        rows.extend(kept)
        last_ts = batch[-1][0]

        if batch_n % 50 == 0:
            pct = (last_ts - since_ms) / total_range * 100
            log(f"[{symbol}] batch {batch_n}, last={pd.to_datetime(last_ts, unit='ms', utc=True)}, "
                f"progress={pct:.1f}%")

        next_cursor = last_ts + MINUTE_MS
        if next_cursor <= cursor:
            break
        cursor = next_cursor

    if not rows:
        log(f"[{symbol}] no rows fetched — skipping write")
        return

    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df[["open", "high", "low", "close", "volume"]] = df[
        ["open", "high", "low", "close", "volume"]
    ].astype("float64")
    df = df.drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)

    df.to_parquet(tmp_path, engine="pyarrow", compression="zstd", index=False)
    os.replace(tmp_path, path)
    log(f"[{symbol}] wrote {len(df)} rows → {path}")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    exchange = ccxt.binanceusdm({
        "enableRateLimit": True,
        "options": {"defaultType": "future"},
    })
    for symbol in SYMBOLS:
        download_symbol(exchange, symbol)
    log("done")


if __name__ == "__main__":
    main()
