#!/usr/bin/env python3
"""
Build the mf_daily_returns table: daily NAV returns for Growth option,
Regular plan mutual fund schemes.

Filters applied:
  - Scheme name must contain "Growth" as the plan/option indicator
  - Scheme name must NOT contain "Direct" (excludes direct plans)
  - Dividend / IDCW must NOT appear *after* "Growth" in the name
    (handles fund names like "Nippon India Growth Mid Cap - IDCW Option"
     where "Growth" is part of the fund name, not the option)

Daily return = (nav_t / nav_{t-1}) - 1, computed per scheme ordered by nav_date.
The first NAV date per scheme produces no return row (no prior day available).

Usage:
    python build_mf_daily_returns.py          # full rebuild
    python build_mf_daily_returns.py --append # only insert missing dates
"""

import argparse
import time

from db import DB_PATH, get_connection, init_db

# ── DDL ──────────────────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS mf_daily_returns (
    scheme_code  INTEGER NOT NULL REFERENCES schemes(scheme_code),
    nav_date     DATE    NOT NULL,
    daily_return REAL    NOT NULL,   -- (nav_t / nav_{t-1}) - 1
    PRIMARY KEY (scheme_code, nav_date)
);
"""

_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_mfdr_date   ON mf_daily_returns(nav_date);",
    "CREATE INDEX IF NOT EXISTS idx_mfdr_scheme ON mf_daily_returns(scheme_code);",
]

# ── Scheme filtering ─────────────────────────────────────────────────────────

def _is_growth_regular(name: str) -> bool:
    """
    Return True if the scheme is a Growth option, Regular plan.

    Rules:
    1. "GROWTH" must appear in the name.
    2. "DIRECT" must NOT appear in the name.
    3. "IDCW", "DIVIDEND", or "BONUS" must NOT appear *after* the last "GROWTH"
       occurrence (prevents matching fund names like
       "XYZ Growth Fund - IDCW Option" or "XYZ Growth Plan - Bonus Option").
    """
    n = name.upper()
    if "GROWTH" not in n:
        return False
    if "DIRECT" in n:
        return False
    growth_idx = n.rfind("GROWTH")
    div_idx = max(n.rfind("IDCW"), n.rfind("DIVIDEND"), n.rfind("BONUS"))
    if div_idx > growth_idx:
        return False
    return True


def get_qualifying_scheme_codes(conn) -> list[int]:
    rows = conn.execute("SELECT scheme_code, scheme_name FROM schemes").fetchall()
    return [r["scheme_code"] for r in rows if _is_growth_regular(r["scheme_name"])]


# ── Core computation ─────────────────────────────────────────────────────────

_INSERT_RETURNS = """
INSERT OR IGNORE INTO mf_daily_returns (scheme_code, nav_date, daily_return)
SELECT scheme_code, nav_date, daily_return
FROM (
    SELECT
        scheme_code,
        nav_date,
        (nav / LAG(nav) OVER (PARTITION BY scheme_code ORDER BY nav_date)) - 1
            AS daily_return
    FROM nav_history
    WHERE scheme_code IN ({placeholders})
      AND nav IS NOT NULL
      AND nav > 0
)
WHERE daily_return IS NOT NULL;
"""

# Append-mode SQL: filters nav_history to nav_date >= anchor_date so the window
# function only scans recent rows.  The anchor row itself produces NULL daily_return
# (no preceding row in the filtered set) and is skipped; the first new date uses
# the anchor row as its LAG reference and computes correctly.
_INSERT_RETURNS_APPEND = """
INSERT OR IGNORE INTO mf_daily_returns (scheme_code, nav_date, daily_return)
SELECT scheme_code, nav_date, daily_return
FROM (
    SELECT
        scheme_code,
        nav_date,
        (nav / LAG(nav) OVER (PARTITION BY scheme_code ORDER BY nav_date)) - 1
            AS daily_return
    FROM nav_history
    WHERE scheme_code IN ({placeholders})
      AND nav IS NOT NULL
      AND nav > 0
      AND nav_date >= ?
)
WHERE daily_return IS NOT NULL;
"""

_BATCH_SIZE = 500   # scheme codes per INSERT … IN (…) batch


def _run_batch(conn, codes: list[int], sql_template: str,
               extra_params: list | None = None) -> int:
    ph = ",".join("?" * len(codes))
    sql = sql_template.format(placeholders=ph)
    params = codes + (extra_params or [])
    cur = conn.execute(sql, params)
    return cur.rowcount


def build(conn, scheme_codes: list[int], append: bool) -> int:
    if append:
        sql_tmpl = _INSERT_RETURNS_APPEND
        row = conn.execute("SELECT MAX(nav_date) FROM mf_daily_returns").fetchone()
        anchor = row[0]
        print(f"  Anchor date (LAG reference): {anchor}")
        extra = [anchor]
    else:
        sql_tmpl = _INSERT_RETURNS
        extra = None

    total = 0
    n = len(scheme_codes)
    for i in range(0, n, _BATCH_SIZE):
        batch = scheme_codes[i : i + _BATCH_SIZE]
        with conn:
            inserted = _run_batch(conn, batch, sql_tmpl, extra)
        total += inserted
        done = min(i + _BATCH_SIZE, n)
        print(f"  schemes {done:>5}/{n}  |  rows inserted so far: {total:>10,}", end="\r")
    print()
    return total


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build mf_daily_returns table for Growth/Regular schemes",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db", default=str(DB_PATH), help="Path to SQLite database")
    parser.add_argument(
        "--append",
        action="store_true",
        help="Only insert returns for dates not already present (incremental update)",
    )
    args = parser.parse_args()

    init_db(args.db)
    conn = get_connection(args.db)
    conn.execute("PRAGMA cache_size = -65536;")   # 64 MB page cache
    conn.execute("PRAGMA temp_store = MEMORY;")

    # Ensure table exists
    conn.execute(_CREATE_TABLE)
    for idx_sql in _CREATE_INDEXES:
        conn.execute(idx_sql)
    conn.commit()

    mode = "APPEND (incremental)" if args.append else "FULL REBUILD"
    print(f"Mode: {mode}")

    if not args.append:
        print("Clearing existing mf_daily_returns rows …")
        with conn:
            conn.execute("DELETE FROM mf_daily_returns;")

    print("Identifying qualifying schemes (Growth option, Regular plan) …")
    scheme_codes = get_qualifying_scheme_codes(conn)
    print(f"  {len(scheme_codes):,} qualifying schemes found")

    print("Computing and inserting daily returns …")
    t0 = time.time()
    total_inserted = build(conn, scheme_codes, args.append)
    elapsed = time.time() - t0

    # Summary
    conn.execute("ANALYZE mf_daily_returns;")
    row = conn.execute(
        "SELECT COUNT(*), MIN(nav_date), MAX(nav_date) FROM mf_daily_returns"
    ).fetchone()
    print(f"\nDone in {elapsed:.1f}s")
    print(f"  Rows inserted this run : {total_inserted:,}")
    print(f"  Total rows in table    : {row[0]:,}")
    print(f"  Date range             : {row[1]}  →  {row[2]}")

    conn.close()


if __name__ == "__main__":
    main()
