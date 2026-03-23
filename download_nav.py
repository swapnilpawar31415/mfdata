#!/usr/bin/env python3
"""
Download AMFI NAV history files for a given date range.

Usage:
    python download_nav.py --from 2024-01-01 --to 2024-03-31
    python download_nav.py --from 2024-01-01 --to 2024-03-31 --out-dir ./raw --chunk-days 30

Downloads in chunks (default 30 days) to stay within AMFI portal limits.
Each chunk is saved as  raw/YYYY-MM-DD_YYYY-MM-DD.txt
Existing files are skipped so the script is safe to re-run.
"""

import argparse
import time
from datetime import date, timedelta
from pathlib import Path

import requests

AMFI_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
DEFAULT_CHUNK_DAYS = 30
DEFAULT_DELAY_SECS = 1.5
MIN_RESPONSE_BYTES = 200  # anything smaller is an empty/error response


def _amfi_date(d: date) -> str:
    """Format date as DD-Mon-YYYY (e.g. 01-Jan-2024)."""
    return d.strftime("%d-%b-%Y")


def _download_chunk(frmdt: date, todt: date, out_dir: Path, session: requests.Session) -> bool:
    """
    Download one chunk and save to out_dir.
    Returns True if a new file was saved, False if skipped or empty.
    """
    fname = out_dir / f"{frmdt.isoformat()}_{todt.isoformat()}.txt"

    if fname.exists() and fname.stat().st_size > MIN_RESPONSE_BYTES:
        print(f"  [skip]  {fname.name}  (already exists)")
        return False

    params = {"frmdt": _amfi_date(frmdt), "todt": _amfi_date(todt)}
    try:
        resp = session.get(AMFI_URL, params=params, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [error] {frmdt} → {todt}: {e}")
        return False

    content = resp.text.strip()
    if len(content) < MIN_RESPONSE_BYTES:
        print(f"  [empty] {frmdt} → {todt}  (no data returned)")
        return False

    fname.write_text(content, encoding="utf-8")
    kb = fname.stat().st_size // 1024
    print(f"  [ok]    {fname.name}  ({kb:,} KB)")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download AMFI mutual fund NAV history",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--from",    dest="from_date",   required=True,
                        help="Start date  YYYY-MM-DD")
    parser.add_argument("--to",      dest="to_date",     required=True,
                        help="End date    YYYY-MM-DD")
    parser.add_argument("--out-dir", dest="out_dir",     default="raw",
                        help="Directory to save downloaded files")
    parser.add_argument("--chunk-days", dest="chunk_days", type=int, default=DEFAULT_CHUNK_DAYS,
                        help="Days per download request")
    parser.add_argument("--delay",   dest="delay",       type=float, default=DEFAULT_DELAY_SECS,
                        help="Seconds to wait between requests")
    args = parser.parse_args()

    start = date.fromisoformat(args.from_date)
    end   = date.fromisoformat(args.to_date)

    if start > end:
        parser.error("--from must not be after --to")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    total_days = (end - start).days + 1
    total_chunks = -(-total_days // args.chunk_days)  # ceiling division
    print(f"AMFI NAV download: {start} → {end}  ({total_days} days, ~{total_chunks} chunks)")
    print(f"Output dir : {out_dir.resolve()}")
    print(f"Chunk size : {args.chunk_days} days  |  Delay: {args.delay}s\n")

    session = requests.Session()
    session.headers.update({"User-Agent": "mfdata-downloader/1.0"})

    saved = skipped = errors = 0
    cur = start

    while cur <= end:
        chunk_end = min(cur + timedelta(days=args.chunk_days - 1), end)
        result = _download_chunk(cur, chunk_end, out_dir, session)
        if result:
            saved += 1
        else:
            skipped += 1

        cur = chunk_end + timedelta(days=1)
        if cur <= end:
            time.sleep(args.delay)

    print(f"\nDone — {saved} downloaded, {skipped} skipped.")


if __name__ == "__main__":
    main()
