#!/usr/bin/env python3
"""
load_benchmarks.py  —  Run this on the SERVER after uploading benchmark CSVs.

Reads every CSV in --csv-dir, matches the filename back to an index name,
and upserts rows into the benchmark_returns table.

Expected CSV format (produced by download_benchmarks.py):
    Date,Close
    2006-04-03,2856.44
    2006-04-04,2901.12
    ...

Filenames must match _safe_filename(db_name) from download_benchmarks.py, e.g.:
    NIFTY_500_TRI.csv
    S&P_BSE_500_TRI.csv
    CRISIL_Composite_Bond_Fund_Index.csv

Usage
-----
    python load_benchmarks.py                         # load all CSVs in ./benchmark_csvs/
    python load_benchmarks.py --csv-dir /path/to/dir
    python load_benchmarks.py --replace               # overwrite existing rows
    python load_benchmarks.py --dry-run               # validate without writing
"""

import argparse
import csv
import sqlite3
from pathlib import Path

from db import DB_PATH, get_connection, init_db

# ── Filename → index_name reverse mapping ────────────────────────────────────
# Built from both NIFTY and BSE maps in download_benchmarks.py.
# We reconstruct the safe filename and map it back to the DB index name.

def _safe_filename(name: str) -> str:
    return name.replace("/", "-").replace(":", "-").replace(" ", "_").replace("&", "and") + ".csv"


# All index names used in schemes.benchmark / benchmark_2
ALL_INDEX_NAMES = [
    # NIFTY equity TRI
    "NIFTY 100 TRI",
    "NIFTY 500 TRI",
    "NIFTY 500 Multicap 50:25:25 TRI",
    "NIFTY Large Midcap 250 TRI",
    "NIFTY Midcap 150 TRI",
    "NIFTY Smallcap 250 TRI",
    "NIFTY Dividend Opportunities 50 TRI",
    "NIFTY Auto TRI",
    "NIFTY Bank TRI",
    "NIFTY Commodities TRI",
    "NIFTY Energy TRI",
    "NIFTY FMCG TRI",
    "NIFTY Financial Services TRI",
    "NIFTY Healthcare TRI",
    "NIFTY IT TRI",
    "NIFTY India Consumption TRI",
    "NIFTY India Defence TRI",
    "NIFTY India Manufacturing TRI",
    "NIFTY India Rural Index TRI",
    "NIFTY India Transport & Logistics TRI",
    "NIFTY Infrastructure TRI",
    "NIFTY Pharma TRI",
    "NIFTY PSE TRI",
    "NIFTY 100 ESG TRI",
    "NIFTY Export & Logistic TRI",
    # NIFTY debt
    "NIFTY Composite Debt Index",
    "NIFTY Corporate Bond Index",
    "NIFTY Credit Risk Bond Index",
    "NIFTY Banking and PSU Debt Index",
    "NIFTY All Duration G-Sec Index",
    "NIFTY 10yr Benchmark G-Sec Index",
    "NIFTY Liquid Index A-I",
    "NIFTY 1D Rate Index",
    "NIFTY Money Market Index",
    "NIFTY Short Duration Debt Index",
    "NIFTY Low Duration Debt Index",
    "NIFTY Ultra Short Duration Debt Index",
    "NIFTY Medium Duration Debt Index",
    "NIFTY Medium to Long Duration Debt Index",
    "NIFTY Long Duration Debt Index",
    "NIFTY Floater Long Term Debt Index",
    # NIFTY hybrid / strategy
    "NIFTY 50 Hybrid Composite Debt 15:85 Index",
    "NIFTY 50 Hybrid Composite Debt 50:50 Index",
    "NIFTY 50 Hybrid Composite Debt 65:35 Index",
    "NIFTY Equity Savings Index",
    "NIFTY 50 Arbitrage Index",
    # S&P BSE
    "S&P BSE 100 TRI",
    "S&P BSE 500 TRI",
    "S&P BSE 250 Large MidCap TRI",
    "S&P BSE 250 SmallCap TRI",
    "S&P BSE Midcap 150 TRI",
    "S&P BSE MNC TRI",
    "S&P BSE Auto TRI",
    "S&P BSE Bankex TRI",
    "S&P BSE Financial Services TRI",
    "S&P BSE FMCG TRI",
    "S&P BSE Healthcare TRI",
    "S&P BSE IT TRI",
    "S&P BSE India Infrastructure TRI",
    "S&P BSE India Manufacturing TRI",
    "S&P BSE Energy TRI",
    "S&P BSE Metal TRI",
    "S&P BSE PSU TRI",
    "S&P BSE Commodities TRI",
    "S&P BSE Transport TRI",
    "S&P BSE Dividend Stability TRI",
    "S&P BSE India Consumer Discretionary TRI",
    "S&P BSE 100 ESG TRI",
    "S&P BSE India Defence TRI",
    "S&P BSE Agri Index TRI",
    "S&P BSE Export TRI",
    # CRISIL
    "CRISIL 10 Year Gilt Index",
    "CRISIL Banking and PSU Debt Index",
    "CRISIL Composite Bond Fund Index",
    "CRISIL Corporate Bond Composite Index",
    "CRISIL Dynamic Gilt Index",
    "CRISIL Hybrid 25+75 Conservative Index",
    "CRISIL Hybrid 35+65 Aggressive Index",
    "CRISIL Hybrid 50+50 Moderate Index",
    "CRISIL Hybrid 85+15 Conservative Index",
    "CRISIL Liquid Fund Index",
    "CRISIL Low Duration Debt Index",
    "CRISIL Medium Term Debt Index",
    "CRISIL Money Market Index",
    "CRISIL Overnight Index",
    "CRISIL Short Duration Debt Index",
    "CRISIL Short Term Credit Risk Index",
    "CRISIL Ultra Short Term Debt Index",
    # Other
    "Domestic Price of Gold",
]

FILENAME_TO_INDEX = {_safe_filename(n): n for n in ALL_INDEX_NAMES}


# ── SQL ───────────────────────────────────────────────────────────────────────

_INSERT_IGNORE = """
INSERT OR IGNORE INTO benchmark_returns (index_name, index_date, close_value, source)
VALUES (?, ?, ?, ?)
"""

_INSERT_REPLACE = """
INSERT OR REPLACE INTO benchmark_returns (index_name, index_date, close_value, source)
VALUES (?, ?, ?, ?)
"""


def _infer_source(index_name: str) -> str:
    if index_name.startswith("NIFTY") or index_name.startswith("Nifty"):
        return "niftyindices"
    if index_name.startswith("S&P BSE"):
        return "bseindia"
    if index_name.startswith("CRISIL"):
        return "crisil"
    return "manual"


def load_csv(path: Path, index_name: str, conn: sqlite3.Connection,
             replace: bool = False, dry_run: bool = False) -> dict:
    sql    = _INSERT_REPLACE if replace else _INSERT_IGNORE
    source = _infer_source(index_name)
    rows_parsed = 0
    rows_loaded = 0
    errors      = 0

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        batch  = []

        for row in reader:
            rows_parsed += 1
            try:
                # Accept Date column named 'Date', 'DATE', 'date', 'Dates'
                date_val  = (row.get("Date") or row.get("DATE") or
                             row.get("date") or row.get("Dates") or "").strip()
                close_val = (row.get("Close") or row.get("CLOSE") or
                             row.get("close") or row.get("Value") or "").strip()

                if not date_val or not close_val or close_val in ("-", "N/A", ""):
                    continue

                # Normalise date to ISO format
                close_f = float(close_val.replace(",", ""))
                batch.append((index_name, date_val, close_f, source))

            except (ValueError, KeyError):
                errors += 1
                continue

        if not dry_run and batch:
            with conn:
                conn.executemany(sql, batch)
            rows_loaded = len(batch)
        elif dry_run:
            rows_loaded = len(batch)

    return {"parsed": rows_parsed, "loaded": rows_loaded, "errors": errors}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load benchmark CSV files into benchmark_returns table",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--csv-dir", dest="csv_dir", default="benchmark_csvs",
                        help="Directory containing CSV files from download_benchmarks.py")
    parser.add_argument("--db",      default=str(DB_PATH),
                        help="Path to SQLite database")
    parser.add_argument("--replace", action="store_true",
                        help="Replace existing rows (default: skip duplicates)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and validate without writing to DB")
    args = parser.parse_args()

    csv_dir = Path(args.csv_dir)
    if not csv_dir.exists():
        print(f"Error: directory not found: {csv_dir}")
        return

    csv_files = sorted(csv_dir.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {csv_dir.resolve()}")
        return

    init_db(args.db)
    conn = get_connection(args.db)

    print(f"Loading {len(csv_files)} CSV file(s) into {args.db}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'REPLACE' if args.replace else 'IGNORE duplicates'}\n")

    unrecognised = []
    total_parsed = total_loaded = 0

    for path in csv_files:
        index_name = FILENAME_TO_INDEX.get(path.name)
        if not index_name:
            unrecognised.append(path.name)
            continue

        stats = load_csv(path, index_name, conn, replace=args.replace, dry_run=args.dry_run)
        total_parsed += stats["parsed"]
        total_loaded += stats["loaded"]
        print(f"  {path.name:<55}  {stats['loaded']:>7,} rows  ({stats['errors']} errors)")

    conn.close()

    if unrecognised:
        print(f"\nUnrecognised files (skipped — not in ALL_INDEX_NAMES):")
        for f in unrecognised:
            print(f"  {f}")

    print(f"\nTotal: {total_loaded:,} rows loaded from {len(csv_files) - len(unrecognised)} files")

    if not args.dry_run:
        conn2 = get_connection(args.db)
        cur   = conn2.cursor()
        cur.execute("SELECT COUNT(DISTINCT index_name) FROM benchmark_returns")
        n_idx = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM benchmark_returns")
        n_rows = cur.fetchone()[0]
        cur.execute("SELECT MIN(index_date), MAX(index_date) FROM benchmark_returns")
        dr = cur.fetchone()
        conn2.close()
        print(f"\nDB summary  — benchmark_returns:")
        print(f"  Indices with data : {n_idx}")
        print(f"  Total rows        : {n_rows:,}")
        print(f"  Date range        : {dr[0]}  →  {dr[1]}")


if __name__ == "__main__":
    main()
