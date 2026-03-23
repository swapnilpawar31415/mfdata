#!/usr/bin/env python3
"""
build_excess_returns.py — Add/fill excess_return column in mf_daily_returns.

excess_return = fund_daily_return − benchmark_daily_return

Assignment rules
----------------
  • Benchmark looked up from schemes.benchmark.
  • Exception: "Equity Scheme - Mid Cap Fund" → NIFTY Midcap 100 TRI.

Filling rules
-------------
  • If the scheme has no benchmark, or the benchmark has no data in
    benchmark_daily_returns → excess_return stays NULL.
  • nav_date < benchmark's first date  →  NULL  (fund predates benchmark)
  • nav_date > benchmark's last date   →  NULL  (benchmark expired before fund)
  • nav_date is within [bm_first, bm_last] but has no entry (holiday / weekend)
    → treat benchmark return as 0, so excess_return = fund_return.

Usage
-----
    python build_excess_returns.py          # full rebuild (clears then refills)
    python build_excess_returns.py --append # only fill rows still NULL
"""

import argparse
import time

from db import DB_PATH, get_connection

# ── Benchmark overrides ───────────────────────────────────────────────────────
# Maps scheme category → benchmark name, overriding schemes.benchmark.
# Add entries here as more benchmark data becomes available.

CATEGORY_BENCHMARK_OVERRIDE: dict[str, str] = {
    "Equity Scheme - Mid Cap Fund": "NIFTY Midcap 100 TRI",
}

_BATCH_SIZE = 200   # scheme codes per UPDATE … IN (…) statement


# ── Schema helpers ────────────────────────────────────────────────────────────

def ensure_column(conn) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(mf_daily_returns)")}
    if "excess_return" not in cols:
        conn.execute("ALTER TABLE mf_daily_returns ADD COLUMN excess_return REAL")
        conn.commit()
        print("Added column: excess_return")
    else:
        print("Column excess_return already present.")


# ── Data helpers ──────────────────────────────────────────────────────────────

def get_available_benchmarks(conn) -> set[str]:
    return {r[0] for r in conn.execute(
        "SELECT DISTINCT index_name FROM benchmark_daily_returns"
    ).fetchall()}


def get_benchmark_range(conn, bm_name: str) -> tuple[str, str]:
    row = conn.execute(
        "SELECT MIN(index_date), MAX(index_date) "
        "FROM benchmark_daily_returns WHERE index_name = ?",
        (bm_name,),
    ).fetchone()
    return row[0], row[1]


def get_scheme_benchmark_map(conn) -> dict[int, str]:
    """
    Return {scheme_code: effective_benchmark_name} for every scheme that has
    rows in mf_daily_returns.  CATEGORY_BENCHMARK_OVERRIDE takes priority.
    """
    rows = conn.execute(
        "SELECT s.scheme_code, s.benchmark, s.category "
        "FROM schemes s "
        "INNER JOIN (SELECT DISTINCT scheme_code FROM mf_daily_returns) m "
        "  USING(scheme_code)"
    ).fetchall()

    result: dict[int, str] = {}
    for row in rows:
        sc, bm, cat = row["scheme_code"], row["benchmark"], row["category"]
        effective = CATEGORY_BENCHMARK_OVERRIDE.get(cat, bm)
        if effective:
            result[sc] = effective
    return result


# ── Core update ───────────────────────────────────────────────────────────────

_UPDATE_SQL = """
UPDATE mf_daily_returns
SET excess_return = CASE
    WHEN nav_date < ?  THEN NULL
    WHEN nav_date > ?  THEN NULL
    ELSE daily_return - COALESCE(
        (SELECT bdr.daily_return
         FROM benchmark_daily_returns bdr
         WHERE bdr.index_name = ?
           AND bdr.index_date = nav_date),
        0.0
    )
END
WHERE scheme_code IN ({ph}){null_filter}
"""


def build(
    conn,
    scheme_bm_map: dict[int, str],
    available_bms: set[str],
    append: bool,
) -> None:
    # Group scheme_codes by their effective benchmark
    bm_to_schemes: dict[str, list[int]] = {}
    for sc, bm in scheme_bm_map.items():
        bm_to_schemes.setdefault(bm, []).append(sc)

    null_filter = "\n  AND excess_return IS NULL" if append else ""

    for bm_name in sorted(bm_to_schemes):
        scheme_codes = bm_to_schemes[bm_name]

        if bm_name not in available_bms:
            print(f"  [no data ] {bm_name:<52}  {len(scheme_codes):>5} schemes")
            continue

        bm_min, bm_max = get_benchmark_range(conn, bm_name)
        total = 0

        for i in range(0, len(scheme_codes), _BATCH_SIZE):
            batch = scheme_codes[i : i + _BATCH_SIZE]
            ph    = ",".join("?" * len(batch))
            sql   = _UPDATE_SQL.format(ph=ph, null_filter=null_filter)
            params = [bm_min, bm_max, bm_name] + batch
            with conn:
                cur = conn.execute(sql, params)
            total += cur.rowcount

        print(f"  [updated ] {bm_name:<52}  {len(scheme_codes):>5} schemes  "
              f"{total:>9,} rows")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Add/fill excess_return in mf_daily_returns",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db", default=str(DB_PATH), help="Path to SQLite database")
    parser.add_argument(
        "--append",
        action="store_true",
        help="Only fill rows where excess_return IS NULL (incremental update)",
    )
    args = parser.parse_args()

    conn = get_connection(args.db)
    conn.execute("PRAGMA cache_size = -65536;")
    conn.execute("PRAGMA temp_store = MEMORY;")

    ensure_column(conn)

    mode = "APPEND (NULL rows only)" if args.append else "FULL REBUILD"
    print(f"Mode: {mode}")

    if not args.append:
        print("Resetting all excess_return values to NULL …")
        with conn:
            conn.execute("UPDATE mf_daily_returns SET excess_return = NULL")

    available_bms = get_available_benchmarks(conn)
    print(f"Benchmarks with data in DB  : {len(available_bms)}")

    print("Resolving scheme → benchmark mapping …")
    scheme_bm_map = get_scheme_benchmark_map(conn)
    bm_counts: dict[str, int] = {}
    for bm in scheme_bm_map.values():
        bm_counts[bm] = bm_counts.get(bm, 0) + 1
    print(f"  {len(scheme_bm_map):,} schemes with an effective benchmark "
          f"({len(bm_counts)} distinct benchmarks)")

    print("\nUpdating excess returns:")
    t0 = time.time()
    build(conn, scheme_bm_map, available_bms, args.append)
    elapsed = time.time() - t0

    # Summary
    row = conn.execute(
        "SELECT COUNT(*), COUNT(excess_return) FROM mf_daily_returns"
    ).fetchone()
    total, filled = row[0], row[1]

    print(f"\nDone in {elapsed:.1f}s")
    print(f"  Total rows in table         : {total:,}")
    print(f"  Rows with excess_return     : {filled:,}  ({100*filled/total:.1f}%)")
    print(f"  Rows without excess_return  : {total - filled:,}")

    conn.close()


if __name__ == "__main__":
    main()
