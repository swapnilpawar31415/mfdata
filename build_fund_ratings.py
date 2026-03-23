#!/usr/bin/env python3
"""
build_fund_ratings.py — Rate funds within their peer group.

Active funds  (from fund_performance)
--------------------------------------
Metric: 50% IR percentile + 50% t_stat_alpha percentile  (higher = better).
Peer group: SEBI category (e.g. 'Equity Scheme - Large Cap Fund').

Index funds  (from index_fund_performance)
-------------------------------------------
Metric: tracking_error percentile  (lower TE = higher percentile = better).
Peer group: tracked index (e.g. 'NIFTY 50 TRI').
ir_pct / tstat_pct columns are both set to the TE percentile for uniformity.

Both types share the same fund_ratings table and 0–5 star rating scale:
  rating = avg_pct / 20, rounded to 1 dp, capped at [0.0, 5.0]
  A fund at the very top of its peer group scores 100 → 5.0 stars.

Output
------
Populates fund_ratings:
  scheme_code, category, n_peers, ir_pct, tstat_pct, avg_pct, rating

Usage
-----
    python build_fund_ratings.py
    python build_fund_ratings.py --min-peers 3
"""

import argparse

import pandas as pd

from db import DB_PATH, get_connection

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS fund_ratings (
    scheme_code  INTEGER PRIMARY KEY REFERENCES schemes(scheme_code),
    category     TEXT    NOT NULL,   -- SEBI category (active) or tracked index (index funds)
    n_peers      INTEGER NOT NULL,   -- funds in same peer group used for ranking
    ir_pct       REAL    NOT NULL,   -- active: IR percentile; index: TE percentile (inverted)
    tstat_pct    REAL    NOT NULL,   -- active: t-stat percentile; index: TE percentile (inverted)
    avg_pct      REAL    NOT NULL,   -- average percentile
    rating       REAL    NOT NULL,   -- avg_pct / 20, rounded to 1dp, in [0.0, 5.0]
    computed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


# ── Active funds ──────────────────────────────────────────────────────────────

def build_active(conn, min_peers: int) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT fp.scheme_code, s.category,
               fp.ir, fp.t_stat_alpha
        FROM fund_performance fp
        JOIN schemes s USING(scheme_code)
        WHERE fp.ir IS NOT NULL
          AND fp.t_stat_alpha IS NOT NULL
        """,
        conn,
    )
    print(f"Active — loaded {len(df):,} funds across {df['category'].nunique()} categories")

    counts = df.groupby("category")["scheme_code"].transform("count")
    excluded = df[counts < min_peers]["category"].unique()
    if len(excluded):
        print(f"  Skipping {len(excluded)} category(ies) with < {min_peers} peers")
    df = df[counts >= min_peers].copy()
    print(f"  Rating {len(df):,} funds across {df['category'].nunique()} categories")

    df["ir_pct"]    = df.groupby("category")["ir"].rank(method="average", pct=True) * 100
    df["tstat_pct"] = df.groupby("category")["t_stat_alpha"].rank(method="average", pct=True) * 100
    df["avg_pct"]   = 0.5 * df["ir_pct"] + 0.5 * df["tstat_pct"]
    df["rating"]    = (df["avg_pct"] / 20).round(1).clip(0.0, 5.0)
    df["n_peers"]   = df.groupby("category")["scheme_code"].transform("count")

    return df[["scheme_code", "category", "n_peers", "ir_pct", "tstat_pct", "avg_pct", "rating"]]


# ── Index funds ───────────────────────────────────────────────────────────────

def build_index(conn, min_peers: int) -> pd.DataFrame:
    # Check table exists
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='index_fund_performance'"
    ).fetchone()
    if not exists:
        print("Index — index_fund_performance table not found, skipping.")
        return pd.DataFrame()

    df = pd.read_sql_query(
        """
        SELECT scheme_code, tracked_index AS category, tracking_error
        FROM index_fund_performance
        WHERE tracking_error IS NOT NULL
        """,
        conn,
    )
    if df.empty:
        print("Index — no data in index_fund_performance.")
        return df

    print(f"Index  — loaded {len(df):,} funds across {df['category'].nunique()} tracked indices")

    counts = df.groupby("category")["scheme_code"].transform("count")
    excluded = df[counts < min_peers]["category"].unique()
    if len(excluded):
        print(f"  Skipping {len(excluded)} group(s) with < {min_peers} peers")
    df = df[counts >= min_peers].copy()
    print(f"  Rating {len(df):,} funds across {df['category'].nunique()} index groups")

    # Lower tracking error = better = higher percentile: use ascending=False
    df["te_pct"]    = df.groupby("category")["tracking_error"].rank(
                          method="average", ascending=False, pct=True) * 100
    df["ir_pct"]    = df["te_pct"]   # repurposed for uniformity
    df["tstat_pct"] = df["te_pct"]   # repurposed for uniformity
    df["avg_pct"]   = df["te_pct"]
    df["rating"]    = (df["avg_pct"] / 20).round(1).clip(0.0, 5.0)
    df["n_peers"]   = df.groupby("category")["scheme_code"].transform("count")

    return df[["scheme_code", "category", "n_peers", "ir_pct", "tstat_pct", "avg_pct", "rating"]]


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build fund_ratings table (active + index funds)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument(
        "--min-peers", type=int, default=3,
        help="Minimum funds in a peer group to compute ratings",
    )
    args = parser.parse_args()

    conn = get_connection(args.db)
    conn.execute(_CREATE_TABLE)
    conn.commit()

    active = build_active(conn, args.min_peers)
    print()
    index  = build_index(conn, args.min_peers)

    results = pd.concat([active, index], ignore_index=True)

    with conn:
        conn.execute("DELETE FROM fund_ratings")
        conn.executemany(
            """
            INSERT INTO fund_ratings
              (scheme_code, category, n_peers, ir_pct, tstat_pct, avg_pct, rating)
            VALUES
              (:scheme_code, :category, :n_peers, :ir_pct, :tstat_pct, :avg_pct, :rating)
            """,
            results.to_dict("records"),
        )

    total = len(results)
    print(f"\nWrote {total:,} rows to fund_ratings "
          f"({len(active):,} active + {len(index):,} index)\n")

    print(f"{'Category / Tracked index':<55} {'Peers':>5}  {'Rating range'}")
    print("─" * 80)
    # Active categories first, then index groups
    for cat, grp in results.groupby("category"):
        lo, hi = grp["rating"].min(), grp["rating"].max()
        print(f"{cat:<55} {len(grp):>5}  {lo:.1f} – {hi:.1f}")

    conn.close()


if __name__ == "__main__":
    main()
