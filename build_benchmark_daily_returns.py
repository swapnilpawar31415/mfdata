#!/usr/bin/env python3
"""
build_benchmark_daily_returns.py — Compute daily returns from raw close values
in benchmark_returns and populate benchmark_daily_returns.

    daily_return = (close_t / close_{t-1}) - 1

Returns are computed per index, ordered by date.  The first row for each index
produces no return (no prior close available) and is skipped.

Usage
-----
    python build_benchmark_daily_returns.py          # full rebuild
    python build_benchmark_daily_returns.py --append # only insert missing dates
"""

import argparse
import time

from db import DB_PATH, get_connection

# Full INSERT using window function — INSERT OR IGNORE skips existing rows,
# so the same SQL works for both full-rebuild (after DELETE) and append mode.
_INSERT_SQL = """
INSERT OR IGNORE INTO benchmark_daily_returns (index_name, index_date, daily_return)
SELECT index_name, index_date, daily_return
FROM (
    SELECT
        index_name,
        index_date,
        (close_value / LAG(close_value) OVER (
            PARTITION BY index_name ORDER BY index_date
        )) - 1  AS daily_return
    FROM benchmark_returns
    WHERE close_value IS NOT NULL
      AND close_value > 0
)
WHERE daily_return IS NOT NULL;
"""

_PER_INDEX_SQL = """
SELECT
    br.index_name,
    COUNT(bdr.index_date)          AS bdr_rows,
    MAX(bdr.index_date)            AS bdr_latest,
    COUNT(br.index_date)           AS br_rows,
    MAX(br.index_date)             AS br_latest
FROM (SELECT DISTINCT index_name FROM benchmark_returns) n
JOIN benchmark_returns  br  USING(index_name)
LEFT JOIN benchmark_daily_returns bdr
       ON bdr.index_name = br.index_name
      AND bdr.index_date = br.index_date
GROUP BY br.index_name
ORDER BY br.index_name;
"""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build benchmark_daily_returns from benchmark_returns close values",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db", default=str(DB_PATH),
                        help="Path to SQLite database")
    parser.add_argument(
        "--append",
        action="store_true",
        help="Only insert dates not already present (default: full rebuild)",
    )
    args = parser.parse_args()

    conn = get_connection(args.db)

    # ── Before summary ────────────────────────────────────────────────────────
    r_before = conn.execute(
        "SELECT COUNT(DISTINCT index_name), COUNT(*), MAX(index_date) "
        "FROM benchmark_daily_returns"
    ).fetchone()
    r_src = conn.execute(
        "SELECT COUNT(DISTINCT index_name), COUNT(*), MAX(index_date) "
        "FROM benchmark_returns"
    ).fetchone()

    print(f"benchmark_returns            : {r_src[0]} indices, {r_src[1]:,} rows, latest {r_src[2]}")
    print(f"benchmark_daily_returns (pre): {r_before[0]} indices, {r_before[1]:,} rows, latest {r_before[2]}")

    # ── Rebuild or append ─────────────────────────────────────────────────────
    mode = "APPEND (skip existing)" if args.append else "FULL REBUILD"
    print(f"\nMode: {mode}")

    if not args.append:
        print("Clearing benchmark_daily_returns …")
        with conn:
            conn.execute("DELETE FROM benchmark_daily_returns")

    print("Computing daily returns …")
    t0 = time.time()
    with conn:
        cur = conn.execute(_INSERT_SQL)
    elapsed = time.time() - t0

    # ── After summary ─────────────────────────────────────────────────────────
    r_after = conn.execute(
        "SELECT COUNT(DISTINCT index_name), COUNT(*), MIN(index_date), MAX(index_date) "
        "FROM benchmark_daily_returns"
    ).fetchone()

    print(f"\nInserted {cur.rowcount:,} rows in {elapsed:.1f}s")
    print(f"benchmark_daily_returns (post): {r_after[0]} indices, {r_after[1]:,} rows, "
          f"{r_after[2]} → {r_after[3]}")

    # ── Per-index breakdown ───────────────────────────────────────────────────
    print(f"\n{'Index':<45} {'BDR rows':>8}  {'BDR latest':>12}  {'BR latest':>12}")
    print("─" * 82)
    for row in conn.execute(_PER_INDEX_SQL).fetchall():
        idx, bdr_rows, bdr_latest, br_rows, br_latest = row
        lag = "  ✓" if bdr_latest == br_latest else f"  ← lags by {br_rows - bdr_rows} rows"
        print(f"{idx:<45} {bdr_rows:>8,}  {str(bdr_latest):>12}  {str(br_latest):>12}{lag}")

    conn.close()


if __name__ == "__main__":
    main()
