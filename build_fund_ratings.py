#!/usr/bin/env python3
"""
build_fund_ratings.py — Rate funds within their category using IR and t-stat alpha.

Method
------
1. Within each category, compute percentile rank for IR and t_stat_alpha
   (higher = better for both).
2. Average percentile = 0.5 * ir_percentile + 0.5 * tstat_percentile
3. Rating = avg_percentile / 20, rounded to 1 decimal, capped at [0.0, 5.0]

Percentile is computed using fractional rank within category (ties averaged),
giving values in (0, 100].  A fund at the very top of its category scores
100 → rating 5.0.

Output
------
Populates the fund_ratings table:
  scheme_code, category, n_peers, ir_pct, tstat_pct, avg_pct, rating

Usage
-----
    python build_fund_ratings.py
    python build_fund_ratings.py --min-peers 5
"""

import argparse

import pandas as pd

from db import DB_PATH, get_connection

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS fund_ratings (
    scheme_code  INTEGER PRIMARY KEY REFERENCES schemes(scheme_code),
    category     TEXT    NOT NULL,
    n_peers      INTEGER NOT NULL,   -- funds in same category used for ranking
    ir_pct       REAL    NOT NULL,   -- percentile rank on IR within category (0-100]
    tstat_pct    REAL    NOT NULL,   -- percentile rank on t_stat_alpha within category (0-100]
    avg_pct      REAL    NOT NULL,   -- weighted average percentile (50/50)
    rating       REAL    NOT NULL,   -- avg_pct / 20, rounded to 1dp, in [0.0, 5.0]
    computed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def build(conn, min_peers: int) -> pd.DataFrame:
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
    print(f"Loaded {len(df):,} funds across {df['category'].nunique()} categories")

    # Filter out categories with too few peers to rank meaningfully
    counts = df.groupby("category")["scheme_code"].transform("count")
    excluded = df[counts < min_peers]["category"].unique()
    if len(excluded):
        print(f"Skipping {len(excluded)} category(ies) with fewer than {min_peers} peers:")
        for c in excluded:
            print(f"  {c}")
    df = df[counts >= min_peers].copy()
    print(f"Rating {len(df):,} funds across {df['category'].nunique()} categories\n")

    # Percentile ranks within category (pct=True → fraction in (0,1], *100 → percentile)
    df["ir_pct"] = (
        df.groupby("category")["ir"]
        .rank(method="average", pct=True) * 100
    )
    df["tstat_pct"] = (
        df.groupby("category")["t_stat_alpha"]
        .rank(method="average", pct=True) * 100
    )

    df["avg_pct"] = 0.5 * df["ir_pct"] + 0.5 * df["tstat_pct"]
    df["rating"]  = (df["avg_pct"] / 20).round(1).clip(0.0, 5.0)
    df["n_peers"] = df.groupby("category")["scheme_code"].transform("count")

    return df[["scheme_code", "category", "n_peers",
               "ir_pct", "tstat_pct", "avg_pct", "rating"]]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build fund_ratings table from fund_performance",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument(
        "--min-peers", type=int, default=3,
        help="Minimum funds in a category to compute ratings",
    )
    args = parser.parse_args()

    conn = get_connection(args.db)
    conn.execute(_CREATE_TABLE)
    conn.commit()

    results = build(conn, args.min_peers)

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

    print(f"Wrote {len(results):,} rows to fund_ratings\n")

    # Summary per category
    print(f"{'Category':<55} {'Peers':>5}  {'Rating range'}")
    print("-" * 80)
    for cat, grp in results.groupby("category"):
        lo, hi = grp["rating"].min(), grp["rating"].max()
        print(f"{cat:<55} {len(grp):>5}  {lo:.1f} – {hi:.1f}")

    conn.close()


if __name__ == "__main__":
    main()
