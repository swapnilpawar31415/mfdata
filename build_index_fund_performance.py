#!/usr/bin/env python3
"""
build_index_fund_performance.py — Compute tracking error and tracking difference
for index funds (category = 'Other Scheme - Index Funds', Growth/Regular plan).

The tracked benchmark is inferred from the scheme name using pattern matching.
Schemes whose benchmark is not in benchmark_daily_returns (Sensex, CRISIL, SDL,
strategy variants) are silently skipped.

Metrics computed per fund (last 5 years, or all available if shorter):
  tracked_index   — inferred from scheme name
  n_days          — trading days where both fund and benchmark have data
  date_from/to    — window boundaries
  tracking_error  — annualised std(r_fund − r_bm) × √250
                    lower is better: fund closely replicates the index
  tracking_diff   — annualised mean(r_fund − r_bm) × 250
                    typically slightly negative (expense ratio drag)

Days where the benchmark has no entry (market holiday) are excluded via
inner join, keeping the error measure clean.

Usage
-----
    python build_index_fund_performance.py
    python build_index_fund_performance.py --min-days 60
"""

import argparse
import time
from datetime import date, timedelta

import numpy as np
import pandas as pd

from db import DB_PATH, get_connection

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS index_fund_performance (
    scheme_code     INTEGER PRIMARY KEY REFERENCES schemes(scheme_code),
    tracked_index   TEXT    NOT NULL,
    n_days          INTEGER NOT NULL,
    date_from       DATE    NOT NULL,
    date_to         DATE    NOT NULL,
    tracking_error  REAL,   -- annualised std(r_fund - r_bm) * sqrt(250); lower = better
    tracking_diff   REAL,   -- annualised mean(r_fund - r_bm) * 250; negative = expense drag
    computed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# ── Benchmark inference ───────────────────────────────────────────────────────
# Patterns checked in order — more specific entries must precede shorter ones.
# Key ordering rules:
#   "NIFTY 500 MULTICAP" before "NIFTY 500" before "NIFTY 50"
#   Composite names (LARGE MIDCAP, MIDSMALLCAP) before plain MIDCAP/SMALLCAP numbers

BENCHMARK_PATTERNS: list[tuple[str, str]] = [
    # ── NIFTY 50 family ───────────────────────────────────────────────────────
    ("NIFTY 50 EQUAL WEIGHT",           "NIFTY 50 Equal Weight TRI"),
    ("NIFTY50 EQUAL WEIGHT",            "NIFTY 50 Equal Weight TRI"),
    ("NIFTY 50 EQ WEIGHT",              "NIFTY 50 Equal Weight TRI"),
    ("NIFTY50 EQ WEIGHT",               "NIFTY 50 Equal Weight TRI"),
    ("NIFTY 50 EQL WGT",                "NIFTY 50 Equal Weight TRI"),
    ("NIFTY NEXT 50",                   "NIFTY Next 50 TRI"),
    # ── NIFTY 500 family (must precede "NIFTY 50") ───────────────────────────
    ("NIFTY 500 MULTICAP",              "NIFTY 500 Multicap 50:25:25 TRI"),
    ("NIFTY500 MULTICAP",               "NIFTY 500 Multicap 50:25:25 TRI"),
    ("NIFTY 500",                       "NIFTY 500 TRI"),
    # ── Other broad ───────────────────────────────────────────────────────────
    ("NIFTY TOTAL MARKET",              "NIFTY Total Market TRI"),
    ("NIFTY TOTALMARKET",               "NIFTY Total Market TRI"),
    ("NIFTY 200",                       "NIFTY 200 TRI"),
    ("NIFTY 100",                       "NIFTY 100 TRI"),
    # ── Composite mid/small (specific names before plain numbers) ─────────────
    ("NIFTY LARGE MIDCAP 250",          "NIFTY Large Midcap 250 TRI"),
    ("NIFTY LARGEMIDCAP 250",           "NIFTY Large Midcap 250 TRI"),
    ("NIFTY LARGEMID 250",              "NIFTY Large Midcap 250 TRI"),
    ("NIFTY MIDSMALLCAP 400",           "NIFTY MidSmallcap 400 TRI"),
    ("NIFTY MIDSML 400",                "NIFTY MidSmallcap 400 TRI"),
    ("NIFTY MID SMALL 400",             "NIFTY MidSmallcap 400 TRI"),
    ("NIFTY MICROCAP 250",              "NIFTY Microcap 250 TRI"),
    # ── Midcap ────────────────────────────────────────────────────────────────
    ("NIFTY MIDCAP 150",                "NIFTY Midcap 150 TRI"),
    ("NIFTY MIDCAP 100",                "NIFTY Midcap 100 TRI"),
    ("NIFTY MIDCAP 50",                 "NIFTY Midcap 50 TRI"),
    # ── Smallcap ──────────────────────────────────────────────────────────────
    ("NIFTY SMALLCAP 250",              "NIFTY Smallcap 250 TRI"),
    ("NIFTY SMALLCAP 100",              "NIFTY Smallcap 100 TRI"),
    ("NIFTY SMALLCAP 50",               "NIFTY Smallcap 50 TRI"),
    # ── Sectoral (available via Yahoo Finance) ────────────────────────────────
    ("NIFTY FINANCIAL SERVICES",        "NIFTY Financial Services TRI"),
    ("NIFTY FIN SERVICE",               "NIFTY Financial Services TRI"),
    ("NIFTY INFRASTRUCTURE",            "NIFTY Infrastructure TRI"),
    ("NIFTY PHARMA",                    "NIFTY Pharma TRI"),
    ("NIFTY FMCG",                      "NIFTY FMCG TRI"),
    ("NIFTY ENERGY",                    "NIFTY Energy TRI"),
    ("NIFTY AUTO",                      "NIFTY Auto TRI"),
    ("NIFTY METAL",                     "NIFTY Metal TRI"),
    ("NIFTY REALTY",                    "NIFTY Realty TRI"),
    ("NIFTY PSE",                       "NIFTY PSE TRI"),
    ("NIFTY COMMODITIES",               "NIFTY Commodities TRI"),
    ("NIFTY BANK",                      "NIFTY Bank TRI"),
    ("NIFTY IT",                        "NIFTY IT TRI"),
    # ── NIFTY 50 — last, most generic ────────────────────────────────────────
    ("NIFTY 50",                        "NIFTY 50 TRI"),
]


def infer_benchmark(scheme_name: str) -> str | None:
    n = scheme_name.upper()
    for pattern, bm in BENCHMARK_PATTERNS:
        if pattern in n:
            return bm
    return None   # Sensex, CRISIL, SDL, strategy variants → skip


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build index_fund_performance table (tracking error / diff)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db",       default=str(DB_PATH))
    parser.add_argument("--min-days", type=int, default=60,
                        help="Minimum trading days required to include a fund")
    args = parser.parse_args()

    conn = get_connection(args.db)
    conn.execute(_CREATE_TABLE)
    conn.commit()

    # ── 5-year trailing window ────────────────────────────────────────────────
    latest_date = conn.execute(
        "SELECT MAX(nav_date) FROM mf_daily_returns"
    ).fetchone()[0]
    cutoff = (date.fromisoformat(latest_date) - timedelta(days=5 * 365)).isoformat()
    print(f"Latest date : {latest_date}")
    print(f"5-yr cutoff : {cutoff}  (funds started later use all available data)")
    print(f"Min days    : {args.min_days}\n")

    # ── Benchmark assignment ──────────────────────────────────────────────────
    schemes = conn.execute(
        """
        SELECT s.scheme_code, s.scheme_name
        FROM schemes s
        WHERE s.category = 'Other Scheme - Index Funds'
          AND EXISTS (SELECT 1 FROM mf_daily_returns m WHERE m.scheme_code = s.scheme_code)
        """
    ).fetchall()

    avail_bms = {r[0] for r in conn.execute(
        "SELECT DISTINCT index_name FROM benchmark_daily_returns"
    ).fetchall()}

    mapping: dict[int, str] = {}   # scheme_code → tracked_index
    n_no_pattern = 0
    n_no_data    = 0

    for sc, name in schemes:
        bm = infer_benchmark(name)
        if bm is None:
            n_no_pattern += 1
        elif bm not in avail_bms:
            n_no_data += 1
        else:
            mapping[sc] = bm

    print(f"Index fund schemes in mf_daily_returns  : {len(schemes)}")
    print(f"  Benchmark matched + data available    : {len(mapping)}")
    print(f"  No pattern match (Sensex/CRISIL/SDL)  : {n_no_pattern}")
    print(f"  Pattern matched but no BM data        : {n_no_data}")

    if not mapping:
        print("\nNothing to compute.")
        return

    # ── Load data ─────────────────────────────────────────────────────────────
    sc_list  = list(mapping.keys())
    bm_list  = list(set(mapping.values()))
    sc_ph    = ",".join("?" * len(sc_list))
    bm_ph    = ",".join("?" * len(bm_list))

    print("\nLoading data …")
    fund_df = pd.read_sql_query(
        f"""
        SELECT scheme_code, nav_date, daily_return
        FROM mf_daily_returns
        WHERE scheme_code IN ({sc_ph})
          AND nav_date >= ?
        """,
        conn,
        params=sc_list + [cutoff],
    )
    fund_df["tracked_index"] = fund_df["scheme_code"].map(mapping)

    bm_df = pd.read_sql_query(
        f"""
        SELECT index_name, index_date, daily_return AS bm_return
        FROM benchmark_daily_returns
        WHERE index_name IN ({bm_ph})
          AND index_date >= ?
        """,
        conn,
        params=bm_list + [cutoff],
    )

    print(f"  {len(fund_df):,} fund-day rows  ({fund_df['scheme_code'].nunique()} schemes)")
    print(f"  {len(bm_df):,} benchmark-day rows  ({bm_df['index_name'].nunique()} indices)")

    # Inner join: only days where both fund and benchmark have data
    merged = fund_df.merge(
        bm_df,
        left_on=["tracked_index", "nav_date"],
        right_on=["index_name",   "index_date"],
        how="inner",
    )
    merged["excess"] = merged["daily_return"] - merged["bm_return"]

    # ── Compute per-scheme metrics ────────────────────────────────────────────
    print("Computing tracking metrics …")
    t0 = time.time()

    results = []
    for sc, grp in merged.groupby("scheme_code"):
        grp = grp.sort_values("nav_date")
        n   = len(grp)
        if n < args.min_days:
            continue
        results.append({
            "scheme_code":   int(sc),
            "tracked_index": mapping[sc],
            "n_days":        n,
            "date_from":     grp["nav_date"].iloc[0],
            "date_to":       grp["nav_date"].iloc[-1],
            "tracking_error": float(grp["excess"].std()  * np.sqrt(250)),
            "tracking_diff":  float(grp["excess"].mean() * 250),
        })

    elapsed = time.time() - t0
    print(f"  {len(results)} schemes computed in {elapsed:.1f}s\n")

    # ── Write ─────────────────────────────────────────────────────────────────
    with conn:
        conn.execute("DELETE FROM index_fund_performance")
        conn.executemany(
            """
            INSERT INTO index_fund_performance
              (scheme_code, tracked_index, n_days, date_from, date_to,
               tracking_error, tracking_diff)
            VALUES
              (:scheme_code, :tracked_index, :n_days, :date_from, :date_to,
               :tracking_error, :tracking_diff)
            """,
            results,
        )

    print(f"Wrote {len(results)} rows to index_fund_performance\n")

    # ── Summary per tracked index ─────────────────────────────────────────────
    res_df = pd.DataFrame(results)
    print(f"{'Tracked index':<45} {'Funds':>5}  {'TE range (ann)'}")
    print("─" * 72)
    for idx, grp in res_df.groupby("tracked_index"):
        lo = grp["tracking_error"].min()
        hi = grp["tracking_error"].max()
        print(f"{idx:<45} {len(grp):>5}  {lo:.4f} – {hi:.4f}")

    conn.close()


if __name__ == "__main__":
    main()
