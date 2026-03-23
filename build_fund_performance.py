#!/usr/bin/env python3
"""
build_fund_performance.py — 5-year trailing performance metrics for funds
that have excess returns calculated.

Metrics computed per fund (last 5 years of data, or all available if shorter):
  n_days         — trading days used
  date_from/to   — window boundaries
  beta           — OLS slope: fund_return ~ benchmark_return
  ann_alpha      — annualised mean daily alpha  (mean(r_fund - beta*r_bm) * 250)
  t_stat_alpha   — t-statistic: ann_alpha / annualised_SE_of_mean_alpha
                   = mean(daily_alpha) * sqrt(n) / std(daily_alpha)
                   (equivalent to one-sample t-test: mean_daily_alpha ≠ 0)
  ann_excess_ret — annualised mean daily excess return (mean(r_fund - r_bm) * 250)
  ann_excess_std — annualised std of daily excess returns (std * sqrt(250))
  ir             — information ratio (ann_excess_ret / ann_excess_std)

Note: benchmark_return is reconstructed as daily_return − excess_return.

Usage:
    python build_fund_performance.py
    python build_fund_performance.py --min-days 120
"""

import argparse
import time
from datetime import date, timedelta

import numpy as np
import pandas as pd

from db import DB_PATH, get_connection, init_db

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS fund_performance (
    scheme_code     INTEGER PRIMARY KEY REFERENCES schemes(scheme_code),
    n_days          INTEGER NOT NULL,
    date_from       DATE    NOT NULL,
    date_to         DATE    NOT NULL,
    beta            REAL,
    ann_alpha       REAL,    -- annualised daily alpha  (intercept * 250)
    t_stat_alpha    REAL,    -- t-stat for H0: mean daily alpha = 0
    ann_excess_ret  REAL,    -- annualised mean daily excess return
    ann_excess_std  REAL,    -- annualised std of daily excess returns
    ir              REAL,    -- information ratio
    computed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

_TRADING_DAYS_PER_YEAR = 250


def _compute(group: pd.DataFrame) -> dict | None:
    y  = group["daily_return"].to_numpy(dtype=np.float64)
    er = group["excess_return"].to_numpy(dtype=np.float64)
    x  = y - er          # benchmark_return = fund_return − excess_return
    n  = len(y)

    # ── OLS: y = alpha + beta*x ───────────────────────────────────────────
    x_mean  = x.mean()
    y_mean  = y.mean()
    SS_xx   = ((x - x_mean) ** 2).sum()
    if SS_xx < 1e-14:
        return None      # degenerate: benchmark never moved

    beta  = np.dot(x - x_mean, y - y_mean) / SS_xx
    alpha = y_mean - beta * x_mean   # OLS intercept = mean daily alpha

    # ── Daily alpha series: r_fund − beta * r_bm ─────────────────────────
    daily_alpha     = y - beta * x   # = alpha + OLS residuals
    alpha_std       = daily_alpha.std(ddof=1)
    if alpha_std < 1e-14:
        return None

    # t-stat: one-sample t on daily_alpha (H0: mean = 0)
    # equivalent to annualized_alpha / annualized_SE_of_mean_alpha
    t_stat = alpha * np.sqrt(n) / alpha_std

    # ── Excess return (raw: r_fund − r_bm, no beta adjustment) ───────────
    er_std = er.std(ddof=1)
    ir     = (er.mean() * _TRADING_DAYS_PER_YEAR) / (er_std * np.sqrt(_TRADING_DAYS_PER_YEAR)) \
             if er_std > 1e-14 else None

    return {
        "scheme_code":    int(group.name),
        "n_days":         n,
        "date_from":      group["nav_date"].iloc[0],
        "date_to":        group["nav_date"].iloc[-1],
        "beta":           float(beta),
        "ann_alpha":      float(alpha * _TRADING_DAYS_PER_YEAR),
        "t_stat_alpha":   float(t_stat),
        "ann_excess_ret": float(er.mean() * _TRADING_DAYS_PER_YEAR),
        "ann_excess_std": float(er_std * np.sqrt(_TRADING_DAYS_PER_YEAR)),
        "ir":             float(ir) if ir is not None else None,
    }


def build(conn, min_days: int) -> int:
    latest  = conn.execute("SELECT MAX(nav_date) FROM mf_daily_returns").fetchone()[0]
    cutoff  = (date.fromisoformat(latest) - timedelta(days=5 * 365)).isoformat()

    print(f"Latest date : {latest}")
    print(f"5-yr cutoff : {cutoff}  (funds started later use all available data)")
    print(f"Min days    : {min_days}")

    print("\nLoading data …")
    df = pd.read_sql_query(
        """
        SELECT scheme_code, nav_date, daily_return, excess_return
        FROM mf_daily_returns
        WHERE excess_return IS NOT NULL
          AND nav_date >= ?
        ORDER BY scheme_code, nav_date
        """,
        conn,
        params=(cutoff,),
    )
    print(f"  {len(df):,} rows  |  {df['scheme_code'].nunique():,} schemes")

    # Filter groups below minimum observations
    counts  = df.groupby("scheme_code").size()
    keep    = counts[counts >= min_days].index
    df      = df[df["scheme_code"].isin(keep)]
    print(f"  {df['scheme_code'].nunique():,} schemes after ≥{min_days}-day filter")

    print("Computing metrics …")
    results = (
        df.groupby("scheme_code", sort=False)
          .apply(_compute, include_groups=False)
          .dropna()
          .tolist()
    )
    print(f"  {len(results):,} schemes computed successfully")

    with conn:
        conn.execute("DELETE FROM fund_performance")
        conn.executemany(
            """
            INSERT INTO fund_performance
              (scheme_code, n_days, date_from, date_to,
               beta, ann_alpha, t_stat_alpha,
               ann_excess_ret, ann_excess_std, ir, computed_at)
            VALUES
              (:scheme_code, :n_days, :date_from, :date_to,
               :beta, :ann_alpha, :t_stat_alpha,
               :ann_excess_ret, :ann_excess_std, :ir, CURRENT_TIMESTAMP)
            """,
            results,
        )
    return len(results)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build fund_performance table from 5-year trailing data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db",       default=str(DB_PATH))
    parser.add_argument("--min-days", type=int, default=60,
                        help="Minimum trading days required to compute stats")
    args = parser.parse_args()

    init_db(args.db)
    conn = get_connection(args.db)
    conn.execute("PRAGMA cache_size = -131072;")
    conn.execute("PRAGMA temp_store = MEMORY;")

    conn.execute(_CREATE_TABLE)
    conn.commit()

    t0 = time.time()
    n  = build(conn, args.min_days)
    print(f"\nDone in {time.time() - t0:.1f}s  —  {n:,} rows in fund_performance")

    # Quick distribution summary
    print("\nMetric distribution (fund_performance):")
    print(f"  {'Metric':<20} {'min':>10} {'median':>10} {'max':>10}")
    print(f"  {'-'*52}")
    for col in ("beta", "ann_alpha", "t_stat_alpha", "ann_excess_ret",
                "ann_excess_std", "ir"):
        row = conn.execute(f"""
            SELECT MIN({col}), AVG({col}),
                   (SELECT {col} FROM fund_performance
                    ORDER BY {col} LIMIT 1 OFFSET
                    (SELECT COUNT(*)/2 FROM fund_performance WHERE {col} IS NOT NULL)),
                   MAX({col})
            FROM fund_performance WHERE {col} IS NOT NULL
        """).fetchone()
        if row and row[0] is not None:
            print(f"  {col:<20} {row[0]:>10.4f} {row[2]:>10.4f} {row[3]:>10.4f}")

    conn.close()


if __name__ == "__main__":
    main()
