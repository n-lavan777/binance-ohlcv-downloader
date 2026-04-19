import glob
import os
import pandas as pd

from config import START_DATE, END_DATE, DATA_DIR

EXPECTED_FLOAT_COLS = ["open", "high", "low", "close", "volume"]
COVERAGE_THRESHOLD  = 0.95


def check_file(path):
    df = pd.read_parquet(path)
    name = os.path.basename(path)
    issues = []

    print(f"\n=== {name} ===")
    print(f"rows: {len(df)}")

    if len(df) == 0:
        print("EMPTY FILE")
        issues.append("empty")
        return issues

    ts = df["timestamp"]
    print(f"range: {ts.min()} → {ts.max()}")

    planned_start = pd.Timestamp(START_DATE)
    planned_end = pd.Timestamp(END_DATE)
    planned_minutes = int((planned_end - planned_start).total_seconds() / 60)
    planned_coverage = len(df) / planned_minutes
    print(f"expected ~{planned_minutes} rows for planned range "
          f"({planned_start} → {planned_end}), got {len(df)} ({planned_coverage * 100:.1f}%)")

    late_listed = ts.min() > planned_start + pd.Timedelta(days=1)
    if late_listed:
        actual_minutes = int((ts.max() - ts.min()).total_seconds() / 60) + 1
        actual_coverage = len(df) / actual_minutes
        print(f"  note: pair listed after planned start — actual-range coverage: "
              f"{actual_coverage * 100:.1f}% ({len(df)}/{actual_minutes})")
        if actual_coverage < COVERAGE_THRESHOLD:
            issues.append(f"low coverage ({actual_coverage * 100:.1f}% of actual range)")
    else:
        if planned_coverage < COVERAGE_THRESHOLD:
            issues.append(f"low coverage ({planned_coverage * 100:.1f}% of planned range)")

    dup_count = int(ts.duplicated().sum())
    print(f"duplicates by timestamp: {dup_count}")
    if dup_count > 0:
        issues.append("duplicates")

    is_monotonic = bool(ts.is_monotonic_increasing)
    print(f"monotonic increasing: {is_monotonic}")
    if not is_monotonic:
        issues.append("not monotonic")

    gaps = ts.diff()
    big = gaps[gaps > pd.Timedelta(minutes=5)]
    if len(gaps.dropna()) > 0:
        max_gap = gaps.max()
        max_gap_idx = gaps.idxmax()
        max_gap_at = df.loc[max_gap_idx, "timestamp"]
        max_gap_min = max_gap.total_seconds() / 60
        print(f"gaps > 5min: {len(big)}, max gap: {max_gap_min:.1f} min at {max_gap_at}")
    else:
        print("gaps > 5min: n/a (single row)")

    o, h, l, c, v = df["open"], df["high"], df["low"], df["close"], df["volume"]
    bad_hl = int((h < l).sum())
    bad_h_open = int((h < o).sum())
    bad_h_close = int((h < c).sum())
    bad_l_open = int((l > o).sum())
    bad_l_close = int((l > c).sum())
    bad_nonpos = int(((o <= 0) | (h <= 0) | (l <= 0) | (c <= 0)).sum())
    bad_vol = int((v < 0).sum())
    print(f"OHLC sanity: h<l={bad_hl}, h<open={bad_h_open}, h<close={bad_h_close}, "
          f"l>open={bad_l_open}, l>close={bad_l_close}, price<=0={bad_nonpos}, volume<0={bad_vol}")
    if bad_hl or bad_h_open or bad_h_close or bad_l_open or bad_l_close or bad_nonpos or bad_vol:
        issues.append("ohlc violations")

    print("dtypes:")
    for col, dtype in df.dtypes.items():
        print(f"  {col}: {dtype}")

    ts_ok = str(df["timestamp"].dtype) == "datetime64[ns, UTC]"
    floats_ok = all(str(df[c].dtype) == "float64" for c in EXPECTED_FLOAT_COLS)
    if not ts_ok:
        issues.append(f"timestamp dtype {df['timestamp'].dtype}")
    if not floats_ok:
        issues.append("float dtype mismatch")

    if issues:
        print(f"ISSUES: {', '.join(issues)}")
    else:
        print("OK")
    return issues


def main():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.parquet")))
    if not files:
        print(f"no parquet files in {DATA_DIR}/")
        return

    total = 0
    with_issues = 0
    for path in files:
        total += 1
        if check_file(path):
            with_issues += 1

    print(f"\nchecked {total} files, {with_issues} with issues")


if __name__ == "__main__":
    main()
