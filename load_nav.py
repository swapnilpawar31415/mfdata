#!/usr/bin/env python3
"""
Parse AMFI NAV history files and load them into the MF database.

Usage:
    python load_nav.py                          # load all files in ./raw/
    python load_nav.py --raw-dir /path/to/raw   # load from a specific directory
    python load_nav.py --file raw/2024-01-01_2024-01-31.txt  # load a single file
    python load_nav.py --replace                # overwrite existing nav_history rows

AMFI file format (semicolon-delimited, UTF-8):
  Category header:  "Open Ended Schemes ( Equity Scheme - Large Cap Fund )"
  AMC name line:    "Aditya Birla Sun Life Mutual Fund"
  Column header:    "Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;..."
  Data row:         "120503;Fund Name - Growth;INF209K01VH8;...;437.01;437.01;437.01;24-Feb-2026"
"""

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterator

from db import DB_PATH, get_connection, init_db

# ── AMFI file parsing ────────────────────────────────────────────────────────

_COLUMN_HEADER_PREFIX = "Scheme Code;Scheme Name"
_CATEGORY_PREFIXES = ("Open Ended Schemes", "Close Ended Schemes", "Interval Fund")
_NULL_VALUES = {"-", "N.A.", "N.A", "NA", ""}


def _parse_float(s: str) -> float | None:
    s = s.strip()
    return None if s in _NULL_VALUES else float(s)


def _parse_date(s: str) -> str | None:
    s = s.strip()
    if not s or s in _NULL_VALUES:
        return None
    return datetime.strptime(s, "%d-%b-%Y").date().isoformat()


def _extract_category(line: str) -> tuple[str, str]:
    """
    'Open Ended Schemes ( Equity Scheme - Large Cap Fund )'
    → scheme_type='Open Ended Schemes', category='Equity Scheme - Large Cap Fund'
    """
    line = line.strip()
    if "(" in line and ")" in line:
        scheme_type = line[:line.index("(")].strip()
        category    = line[line.index("(") + 1 : line.rindex(")")].strip()
    else:
        scheme_type = line
        category    = ""
    return scheme_type, category


def parse_amfi_file(path: Path) -> Iterator[dict]:
    """
    Yield one dict per data row:
      scheme_code, scheme_name, isin_growth, isin_div_reinvestment,
      nav, repurchase_price, sale_price, nav_date,
      amc_name, scheme_type, category
    """
    scheme_type = ""
    category    = ""
    amc_name    = ""

    with open(path, encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue

            # ── Category header ──────────────────────────────────────────
            if any(line.startswith(p) for p in _CATEGORY_PREFIXES):
                scheme_type, category = _extract_category(line)
                amc_name = ""
                continue

            # ── Column header row ────────────────────────────────────────
            if line.startswith(_COLUMN_HEADER_PREFIX):
                continue

            # ── Data row: first field must be a numeric scheme code ──────
            parts = line.split(";")
            if len(parts) >= 7 and parts[0].strip().isdigit():
                try:
                    # Scheme Code;Scheme Name;ISIN Growth;ISIN Div Reinvest;NAV;Repurchase;Sale;Date
                    nav_date = _parse_date(parts[7]) if len(parts) > 7 else _parse_date(parts[6])
                    if nav_date is None:
                        continue

                    isin_growth = parts[2].strip() or None
                    isin_div    = parts[3].strip() or None

                    yield {
                        "scheme_code":           int(parts[0].strip()),
                        "scheme_name":           parts[1].strip(),
                        "isin_growth":           isin_growth,
                        "isin_div_reinvestment": isin_div,
                        "nav":                   _parse_float(parts[4]),
                        "repurchase_price":      _parse_float(parts[5]),
                        "sale_price":            _parse_float(parts[6]),
                        "nav_date":              nav_date,
                        "amc_name":              amc_name,
                        "scheme_type":           scheme_type,
                        "category":              category,
                    }
                except (ValueError, IndexError):
                    continue  # skip malformed rows

            else:
                # Non-data, non-header line with content → AMC name
                if not any(line.startswith(p) for p in _CATEGORY_PREFIXES):
                    amc_name = line


# ── Database loading ─────────────────────────────────────────────────────────

_UPSERT_SCHEME = """
INSERT INTO schemes (
    scheme_code, scheme_name, amc_name, scheme_type, category,
    isin_growth, isin_div_reinvestment, first_nav_date, last_nav_date, updated_at
) VALUES (
    :scheme_code, :scheme_name, :amc_name, :scheme_type, :category,
    :isin_growth, :isin_div_reinvestment, :nav_date, :nav_date, CURRENT_TIMESTAMP
)
ON CONFLICT(scheme_code) DO UPDATE SET
    scheme_name           = excluded.scheme_name,
    amc_name              = COALESCE(excluded.amc_name, schemes.amc_name),
    scheme_type           = COALESCE(excluded.scheme_type, schemes.scheme_type),
    category              = COALESCE(excluded.category, schemes.category),
    isin_growth           = COALESCE(excluded.isin_growth, schemes.isin_growth),
    isin_div_reinvestment = COALESCE(excluded.isin_div_reinvestment, schemes.isin_div_reinvestment),
    first_nav_date        = MIN(schemes.first_nav_date, excluded.first_nav_date),
    last_nav_date         = MAX(schemes.last_nav_date, excluded.last_nav_date),
    updated_at            = CURRENT_TIMESTAMP;
"""

_INSERT_NAV_IGNORE = """
INSERT OR IGNORE INTO nav_history (scheme_code, nav_date, nav, repurchase_price, sale_price)
VALUES (:scheme_code, :nav_date, :nav, :repurchase_price, :sale_price);
"""

_INSERT_NAV_REPLACE = """
INSERT OR REPLACE INTO nav_history (scheme_code, nav_date, nav, repurchase_price, sale_price)
VALUES (:scheme_code, :nav_date, :nav, :repurchase_price, :sale_price);
"""


def load_file(path: Path, conn: sqlite3.Connection, replace: bool = False) -> dict:
    """
    Parse one AMFI file and load it into the DB.
    Returns a stats dict: {rows_parsed, schemes_upserted, navs_inserted}.
    """
    nav_sql = _INSERT_NAV_REPLACE if replace else _INSERT_NAV_IGNORE

    rows_parsed = 0
    schemes_seen: set[int] = set()
    nav_batch: list[dict] = []

    BATCH_SIZE = 2000

    def _flush(cursor: sqlite3.Cursor) -> None:
        cursor.executemany(nav_sql, nav_batch)
        nav_batch.clear()

    with conn:
        cur = conn.cursor()
        for row in parse_amfi_file(path):
            rows_parsed += 1

            # Upsert scheme metadata
            if row["scheme_code"] not in schemes_seen:
                cur.execute(_UPSERT_SCHEME, row)
                schemes_seen.add(row["scheme_code"])

            # Accumulate NAV rows
            nav_batch.append({
                "scheme_code":      row["scheme_code"],
                "nav_date":         row["nav_date"],
                "nav":              row["nav"],
                "repurchase_price": row["repurchase_price"],
                "sale_price":       row["sale_price"],
            })

            if len(nav_batch) >= BATCH_SIZE:
                _flush(cur)

        if nav_batch:
            _flush(cur)

    return {
        "rows_parsed":      rows_parsed,
        "schemes_upserted": len(schemes_seen),
    }


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load AMFI NAV history files into the MF database",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--raw-dir", default="raw",
                       help="Directory of .txt files to load")
    group.add_argument("--file",
                       help="Load a single file instead of a directory")
    parser.add_argument("--db",      default=str(DB_PATH),
                        help="Path to SQLite database")
    parser.add_argument("--replace", action="store_true",
                        help="Overwrite existing NAV rows (default: skip)")
    args = parser.parse_args()

    init_db(args.db)
    conn = get_connection(args.db)

    if args.file:
        files = [Path(args.file)]
    else:
        raw_dir = Path(args.raw_dir)
        files = sorted(raw_dir.glob("*.txt"))
        if not files:
            print(f"No .txt files found in {raw_dir.resolve()}")
            return

    print(f"Loading {len(files)} file(s) into {args.db}")
    print(f"Mode: {'REPLACE' if args.replace else 'IGNORE duplicates'}\n")

    total_rows = total_schemes = 0

    for path in files:
        stats = load_file(path, conn, replace=args.replace)
        total_rows    += stats["rows_parsed"]
        total_schemes += stats["schemes_upserted"]   # counts unique per-file; may overlap across files
        print(f"  {path.name:<45}  {stats['rows_parsed']:>8,} rows  |  {stats['schemes_upserted']:>5,} schemes")

    conn.close()
    print(f"\nTotal: {total_rows:,} rows parsed across {len(files)} file(s)")

    # Quick summary query
    conn2 = get_connection(args.db)
    cur = conn2.cursor()
    cur.execute("SELECT COUNT(*) FROM schemes;")
    n_schemes = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM nav_history;")
    n_navs = cur.fetchone()[0]
    cur.execute("SELECT MIN(nav_date), MAX(nav_date) FROM nav_history;")
    date_range = cur.fetchone()
    conn2.close()

    print(f"\nDB summary:")
    print(f"  Schemes     : {n_schemes:,}")
    print(f"  NAV records : {n_navs:,}")
    print(f"  Date range  : {date_range[0]}  →  {date_range[1]}")


if __name__ == "__main__":
    main()
