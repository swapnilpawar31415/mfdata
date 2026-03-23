#!/usr/bin/env python3
"""
fetch_nse_tri.py — Download TRI (Total Return Index) data for NIFTY indices
using the nsepython library (which pulls from niftyindices.com without
requiring a login).

Covers indices not available on Yahoo Finance, plus additional broad/strategy
indices needed for index fund tracking error analysis.

BSE/Sensex indices are NOT available via nsepython (NSE's own API only).

Indices fetched
---------------
  Broad market
    NIFTY 50 TRI                  NIFTY 50
    NIFTY 50 Equal Weight TRI     NIFTY50 EQL WGT
    NIFTY Next 50 TRI             NIFTY NEXT 50
    NIFTY 100 TRI *               NIFTY 100
    NIFTY 200 TRI                 NIFTY 200
    NIFTY 500 TRI *               NIFTY 500
    NIFTY Total Market TRI        NIFTY TOTAL MKT

  Mid / small cap
    NIFTY Midcap 50 TRI           NIFTY MIDCAP 50
    NIFTY Midcap 100 TRI *        NIFTY MIDCAP 100
    NIFTY Midcap 150 TRI *        NIFTY MIDCAP 150
    NIFTY MidSmallcap 400 TRI     NIFTY MIDSML 400
    NIFTY Large Midcap 250 TRI *  NIFTY LARGEMID250
    NIFTY Smallcap 50 TRI         NIFTY SMLCAP 50
    NIFTY Smallcap 100 TRI        NIFTY SMLCAP 100
    NIFTY Smallcap 250 TRI *      NIFTY SMLCAP 250
    NIFTY Microcap 250 TRI        NIFTY MICROCAP250
    NIFTY 500 Multicap 50:25:25 TRI *  NIFTY500 MULTICAP

  (* previously loaded — INSERT OR IGNORE will skip existing rows)

Data is loaded into benchmark_returns (source = 'nsepython').
Run build_benchmark_daily_returns.py --append afterwards to
propagate into benchmark_daily_returns.

Usage
-----
    python fetch_nse_tri.py                      # fetch all, append new rows
    python fetch_nse_tri.py --from 2006-01-01    # custom start date
    python fetch_nse_tri.py --replace            # overwrite existing rows
    python fetch_nse_tri.py --dry-run            # download but don't write to DB
    python fetch_nse_tri.py --index "NIFTY 50 TRI"  # single index
"""

import argparse
import time
from datetime import date, datetime, timedelta

import nsepython

from db import DB_PATH, get_connection

# ── Index map: DB benchmark name → NSE symbol ────────────────────────────────

NSE_INDEX_MAP: dict[str, str] = {
    # Broad market
    "NIFTY 50 TRI":                    "NIFTY 50",
    "NIFTY 50 Equal Weight TRI":       "NIFTY50 EQL WGT",
    "NIFTY Next 50 TRI":               "NIFTY NEXT 50",
    "NIFTY 100 TRI":                   "NIFTY 100",
    "NIFTY 200 TRI":                   "NIFTY 200",
    "NIFTY 500 TRI":                   "NIFTY 500",
    "NIFTY Total Market TRI":          "NIFTY TOTAL MKT",
    # Mid / small cap
    "NIFTY Midcap 50 TRI":             "NIFTY MIDCAP 50",
    "NIFTY Midcap 100 TRI":            "NIFTY MIDCAP 100",
    "NIFTY Midcap 150 TRI":            "NIFTY MIDCAP 150",
    "NIFTY MidSmallcap 400 TRI":       "NIFTY MIDSML 400",
    "NIFTY Large Midcap 250 TRI":      "NIFTY LARGEMID250",
    "NIFTY Smallcap 50 TRI":           "NIFTY SMLCAP 50",
    "NIFTY Smallcap 100 TRI":          "NIFTY SMLCAP 100",
    "NIFTY Smallcap 250 TRI":          "NIFTY SMLCAP 250",
    "NIFTY Microcap 250 TRI":          "NIFTY MICROCAP250",
    "NIFTY 500 Multicap 50:25:25 TRI": "NIFTY500 MULTICAP",
}

DEFAULT_START  = date(2006, 1, 1)
CHUNK_DAYS     = 7300  # rows per API call (~20 years; no limit observed)
DELAY_SECS     = 1.0   # polite delay between requests

_INSERT_IGNORE  = "INSERT OR IGNORE  INTO benchmark_returns (index_name, index_date, close_value, source) VALUES (?, ?, ?, ?)"
_INSERT_REPLACE = "INSERT OR REPLACE INTO benchmark_returns (index_name, index_date, close_value, source) VALUES (?, ?, ?, ?)"


def _nse_date(d: date) -> str:
    """Format date as DD-Mon-YYYY for nsepython (e.g. '01-Jan-2006')."""
    return d.strftime("%d-%b-%Y")


def _parse_nse_date(s: str) -> str:
    """Parse NSE date string '28 Feb 2025' → ISO '2025-02-28'."""
    return datetime.strptime(s.strip(), "%d %b %Y").date().isoformat()


def _date_chunks(start: date, end: date, chunk_days: int):
    cur = start
    while cur <= end:
        yield cur, min(cur + timedelta(days=chunk_days - 1), end)
        cur += timedelta(days=chunk_days)


def _anchor_date(conn, db_name: str) -> date | None:
    """Return the latest index_date already in benchmark_returns for this index."""
    row = conn.execute(
        "SELECT MAX(index_date) FROM benchmark_returns WHERE index_name = ?",
        (db_name,),
    ).fetchone()
    return date.fromisoformat(row[0]) if row[0] else None


def fetch_index(db_name: str, nse_name: str,
                start: date, end: date,
                conn, replace: bool = False,
                dry_run: bool = False) -> dict:
    sql   = _INSERT_REPLACE if replace else _INSERT_IGNORE
    total = 0
    errors = 0

    for chunk_start, chunk_end in _date_chunks(start, end, CHUNK_DAYS):
        try:
            df = nsepython.index_total_returns(
                nse_name, _nse_date(chunk_start), _nse_date(chunk_end)
            )
        except Exception as e:
            print(f"    {chunk_start} → {chunk_end}: ERROR — {e}")
            errors += 1
            time.sleep(DELAY_SECS)
            continue

        if df.empty:
            time.sleep(DELAY_SECS)
            continue

        rows = []
        for _, row in df.iterrows():
            try:
                iso_date  = _parse_nse_date(str(row["Date"]))
                close_val = float(str(row["TotalReturnsIndex"]).replace(",", ""))
                rows.append((db_name, iso_date, close_val, "nsepython"))
            except (ValueError, KeyError):
                continue

        if rows and not dry_run:
            with conn:
                conn.executemany(sql, rows)
        total += len(rows)
        time.sleep(DELAY_SECS)

    return {"rows": total, "errors": errors}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch NIFTY TRI data via nsepython → benchmark_returns",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--from",    dest="from_date", default=None,
                        help="Start date YYYY-MM-DD (default: day after latest row per index)")
    parser.add_argument("--to",      dest="to_date",
                        default=date.today().isoformat(),
                        help="End date YYYY-MM-DD")
    parser.add_argument("--db",      default=str(DB_PATH))
    parser.add_argument("--replace", action="store_true",
                        help="Overwrite existing rows (default: skip duplicates)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Download but do not write to DB")
    parser.add_argument("--index",   dest="index_filter", default=None,
                        help="Fetch only this DB index name")
    args = parser.parse_args()

    end   = date.fromisoformat(args.to_date)
    conn  = get_connection(args.db)

    print(f"Fetching NSE TRI data → {args.db}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'REPLACE' if args.replace else 'IGNORE duplicates'}\n")

    print(f"{'Index':<45} {'Rows':>7}  {'Date range'}")
    print("─" * 75)

    total_rows = 0
    for db_name, nse_name in NSE_INDEX_MAP.items():
        if args.index_filter and db_name != args.index_filter:
            continue

        # Determine start date: explicit flag → latest in DB + 1 day → default
        if args.from_date:
            start = date.fromisoformat(args.from_date)
        else:
            anchor = _anchor_date(conn, db_name)
            start  = (anchor + timedelta(days=1)) if anchor else DEFAULT_START

        if start > end:
            print(f"  {db_name:<43} already up to date")
            continue

        stats = fetch_index(db_name, nse_name, start, end, conn,
                            replace=args.replace, dry_run=args.dry_run)

        date_range = f"{start} → {end}"
        flag = "  (dry run)" if args.dry_run else ""
        print(f"  {db_name:<43} {stats['rows']:>7,}  {date_range}{flag}")
        if stats["errors"]:
            print(f"    ⚠ {stats['errors']} chunk error(s)")
        total_rows += stats["rows"]

    conn.close()
    print(f"\nTotal rows {'(not written)' if args.dry_run else 'loaded'}: {total_rows:,}")

    if not args.dry_run:
        print("\nNext step:")
        print("  python3 build_benchmark_daily_returns.py --append")


if __name__ == "__main__":
    main()
