#!/usr/bin/env python3
"""
fetch_equity_benchmarks.py — Download equity index price data via Yahoo Finance
and load directly into benchmark_returns.

NOTE: These are **price return** indices (not Total Return / TRI).
      Dividend reinvestment is excluded, so returns are understated by ~1-2%/yr
      vs true TRI.  Acceptable as an approximation until TRI data is sourced.

Available indices (13 NIFTY + BSE Sensex):
    NIFTY 50, 100, 500, Bank, IT, Pharma, Auto, FMCG, Infrastructure,
    Metal, Energy, Realty, PSE  +  BSE Sensex

Usage:
    python fetch_equity_benchmarks.py               # download all, load into DB
    python fetch_equity_benchmarks.py --dry-run     # show what would be fetched
    python fetch_equity_benchmarks.py --from 2010-01-01
"""

import argparse
from datetime import date, datetime

import yfinance as yf

from db import DB_PATH, get_connection, init_db

# ── Index map: DB benchmark name → Yahoo Finance ticker ──────────────────────
# Only indices confirmed to have data on Yahoo Finance are listed here.
# BSE equity indices are not available on Yahoo Finance (only Sensex, which
# isn't used as a benchmark in our schemes table).

EQUITY_INDEX_MAP: dict[str, str] = {
    "NIFTY 500 TRI":          "^CRSLDX",   # available from 2005-09-26
    "NIFTY 100 TRI":          "^CNX100",   # available from 2005-11-30
    "NIFTY Bank TRI":         "^NSEBANK",  # available from 2007-09-17
    "NIFTY IT TRI":           "^CNXIT",    # available from 2007-09-17
    "NIFTY Infrastructure TRI": "^CNXINFRA", # available from 2010-07-19
    "NIFTY Realty TRI":       "^CNXREALTY",# available from 2010-07-19
    "NIFTY Pharma TRI":       "^CNXPHARMA",# available from 2011-01-31
    "NIFTY FMCG TRI":         "^CNXFMCG", # available from 2011-01-31
    "NIFTY Energy TRI":       "^CNXENERGY",# available from 2011-01-31
    "NIFTY PSE TRI":          "^CNXPSE",  # available from 2011-01-31
    "NIFTY Auto TRI":         "^CNXAUTO", # available from 2011-07-12
    "NIFTY Metal TRI":        "^CNXMETAL",# available from 2011-07-12
    "NIFTY Commodities TRI":  "^CNXCMDT", # available from ~2012
    "NIFTY Financial Services TRI": "NIFTY_FIN_SERVICE.NS", # available from 2011-09-07
    "NIFTY Midcap 100 TRI":         "NIFTY_MIDCAP_100.NS",  # available from 2005-09-26
}

# Not available on Yahoo Finance (no ticker confirmed):
NOT_AVAILABLE = [
    "NIFTY 500 Multicap 50:25:25 TRI",
    "NIFTY Large Midcap 250 TRI",
    "NIFTY Midcap 150 TRI",
    "NIFTY Smallcap 250 TRI",
    # "NIFTY Financial Services TRI",  # now available via NIFTY_FIN_SERVICE.NS
    "NIFTY Healthcare TRI",
    "NIFTY Dividend Opportunities 50 TRI",
    "NIFTY India Consumption TRI",
    "NIFTY India Defence TRI",
    "NIFTY India Manufacturing TRI",
    "NIFTY India Rural Index TRI",
    "NIFTY India Transport & Logistics TRI",
    "NIFTY 100 ESG TRI",
    "NIFTY Export & Logistic TRI",
    "All S&P BSE equity indices",
]

_INSERT_IGNORE   = "INSERT OR IGNORE INTO benchmark_returns (index_name, index_date, close_value, source) VALUES (?, ?, ?, ?)"
_INSERT_REPLACE  = "INSERT OR REPLACE INTO benchmark_returns (index_name, index_date, close_value, source) VALUES (?, ?, ?, ?)"


def fetch_and_load(index_name: str, ticker: str,
                   start: date, end: date,
                   conn, replace: bool = False,
                   dry_run: bool = False) -> dict:
    sql = _INSERT_REPLACE if replace else _INSERT_IGNORE

    df = yf.download(ticker, start=str(start), end=str(end),
                     progress=False, auto_adjust=True)

    if df.empty:
        return {"ticker": ticker, "rows": 0, "from": None, "to": None}

    # yfinance returns a MultiIndex column when downloading single ticker
    close = df["Close"]
    if hasattr(close, "iloc") and close.ndim > 1:
        close = close.iloc[:, 0]

    rows = [
        (index_name, idx.date().isoformat(), float(val), "yahoo_finance")
        for idx, val in close.items()
        if val is not None and str(val) not in ("nan", "None")
    ]

    if not dry_run and rows:
        with conn:
            conn.executemany(sql, rows)

    return {
        "ticker": ticker,
        "rows":   len(rows),
        "from":   rows[0][1]  if rows else None,
        "to":     rows[-1][1] if rows else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch equity benchmark price index data from Yahoo Finance → DB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--from",    dest="from_date",
                        default="2005-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--to",      dest="to_date",
                        default=date.today().isoformat(), help="End date YYYY-MM-DD")
    parser.add_argument("--db",      default=str(DB_PATH))
    parser.add_argument("--replace", action="store_true",
                        help="Overwrite existing rows (default: skip duplicates)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Download but do not write to DB")
    args = parser.parse_args()

    start = date.fromisoformat(args.from_date)
    end   = date.fromisoformat(args.to_date)

    init_db(args.db)
    conn = get_connection(args.db)

    print(f"Fetching equity benchmark data: {start} → {end}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'REPLACE' if args.replace else 'IGNORE duplicates'}")
    print(f"NOTE: price return indices (not TRI)\n")

    total_rows = 0
    print(f"{'Index name':<35} {'Ticker':<12} {'Rows':>7}  {'Date range'}")
    print("─" * 80)

    for index_name, ticker in EQUITY_INDEX_MAP.items():
        stats = fetch_and_load(index_name, ticker, start, end, conn,
                               replace=args.replace, dry_run=args.dry_run)
        date_range = (f"{stats['from']} → {stats['to']}"
                      if stats["from"] else "no data")
        print(f"{index_name:<35} {ticker:<12} {stats['rows']:>7,}  {date_range}")
        total_rows += stats["rows"]

    conn.close()

    print(f"\nTotal rows {'(not written — dry run)' if args.dry_run else 'loaded'}: {total_rows:,}")

    if not args.dry_run:
        print("\nNot available on Yahoo Finance (skipped):")
        for name in NOT_AVAILABLE:
            print(f"  • {name}")

        conn2 = get_connection(args.db)
        cur   = conn2.cursor()
        cur.execute("SELECT COUNT(DISTINCT index_name) FROM benchmark_returns")
        n_idx = cur.fetchone()[0]
        cur.execute("SELECT MIN(index_date), MAX(index_date) FROM benchmark_returns")
        dr = cur.fetchone()
        conn2.close()
        print(f"\nDB — benchmark_returns: {n_idx} indices, {dr[0]} → {dr[1]}")


if __name__ == "__main__":
    main()
