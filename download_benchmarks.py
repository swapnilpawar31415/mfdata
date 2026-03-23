#!/usr/bin/env python3
"""
download_benchmarks.py  —  Run this on your LOCAL machine, NOT the server.

Downloads historical daily closing values for all benchmark indices used in
the MF database (NIFTY TRI, S&P BSE, CRISIL) and saves them as CSV files
in ./benchmark_csvs/.  Once downloaded, rsync / scp the folder to the server
and run  load_benchmarks.py  to import into mf.db.

Sources
-------
  NIFTY indices  → niftyindices.com   (official NSE TRI data, free, no login)
  S&P BSE indices → bseindia.com      (BSE historical index archive)
  CRISIL indices → *** manual ***     see CRISIL section at the bottom

Usage
-----
    pip install requests
    python download_benchmarks.py                     # download everything
    python download_benchmarks.py --from 2006-01-01   # custom start date
    python download_benchmarks.py --index "NIFTY 500 TRI"  # single index
    python download_benchmarks.py --source nifty      # only NIFTY indices
    python download_benchmarks.py --source bse        # only BSE indices
"""

import argparse
import csv
import json
import time
from datetime import date, timedelta
from pathlib import Path

import requests

OUT_DIR          = Path("benchmark_csvs")
DEFAULT_START    = date(2006, 1, 1)
CHUNK_DAYS       = 90       # days per API request
DELAY_SECS       = 1.5      # polite delay between requests
MIN_BYTES        = 50       # treat smaller responses as empty

# ── NIFTY index name mapping ──────────────────────────────────────────────────
# Maps our DB benchmark name → exact name used by niftyindices.com API.
# Equity TRI indices use "Total Returns Index" suffix on niftyindices.com.
# Debt/hybrid/arbitrage indices use the same name without the suffix.

NIFTY_INDEX_MAP: dict[str, str] = {
    # Broad equity TRI
    "NIFTY 100 TRI":                          "Nifty 100 Total Returns Index",
    "NIFTY 500 TRI":                          "Nifty 500 Total Returns Index",
    "NIFTY 500 Multicap 50:25:25 TRI":        "Nifty 500 Multicap 50:25:25 Total Returns Index",
    "NIFTY Large Midcap 250 TRI":             "Nifty Large Midcap 250 Total Returns Index",
    "NIFTY Midcap 150 TRI":                   "Nifty Midcap 150 Total Returns Index",
    "NIFTY Smallcap 250 TRI":                 "Nifty Smallcap 250 Total Returns Index",
    "NIFTY Dividend Opportunities 50 TRI":    "Nifty Dividend Opportunities 50 Total Returns Index",

    # Sectoral equity TRI
    "NIFTY Auto TRI":                         "Nifty Auto Total Returns Index",
    "NIFTY Bank TRI":                         "Nifty Bank Total Returns Index",
    "NIFTY Commodities TRI":                  "Nifty Commodities Total Returns Index",
    "NIFTY Energy TRI":                       "Nifty Energy Total Returns Index",
    "NIFTY FMCG TRI":                         "Nifty FMCG Total Returns Index",
    "NIFTY Financial Services TRI":           "Nifty Financial Services Total Returns Index",
    "NIFTY Healthcare TRI":                   "Nifty Healthcare Total Returns Index",
    "NIFTY IT TRI":                           "Nifty IT Total Returns Index",
    "NIFTY India Consumption TRI":            "Nifty India Consumption Total Returns Index",
    "NIFTY India Defence TRI":                "Nifty India Defence Total Returns Index",
    "NIFTY India Manufacturing TRI":          "Nifty India Manufacturing Total Returns Index",
    "NIFTY India Rural Index TRI":            "Nifty India Rural Total Returns Index",
    "NIFTY India Transport & Logistics TRI":  "Nifty India Transport & Logistics Total Returns Index",
    "NIFTY Infrastructure TRI":               "Nifty Infrastructure Total Returns Index",
    "NIFTY Pharma TRI":                       "Nifty Pharma Total Returns Index",
    "NIFTY PSE TRI":                          "Nifty PSE Total Returns Index",
    "NIFTY 100 ESG TRI":                      "Nifty100 ESG Sector Leaders Total Returns Index",
    "NIFTY Export & Logistic TRI":            "Nifty Export and Logistic Total Returns Index",

    # Debt indices (no "Total Returns" suffix — debt indices are already TR by nature)
    "NIFTY Composite Debt Index":             "Nifty Composite Debt Index",
    "NIFTY Corporate Bond Index":             "Nifty Corporate Bond Index",
    "NIFTY Credit Risk Bond Index":           "Nifty Credit Risk Bond Index",
    "NIFTY Banking and PSU Debt Index":       "Nifty Banking and PSU Debt Index",
    "NIFTY All Duration G-Sec Index":         "Nifty All Duration G-Sec Index",
    "NIFTY 10yr Benchmark G-Sec Index":       "Nifty 10yr Benchmark G-Sec Index",
    "NIFTY Liquid Index A-I":                 "Nifty Liquid Index A-I",
    "NIFTY 1D Rate Index":                    "Nifty 1D Rate Index",
    "NIFTY Money Market Index":               "Nifty Money Market Index",
    "NIFTY Short Duration Debt Index":        "Nifty Short Duration Debt Index",
    "NIFTY Low Duration Debt Index":          "Nifty Low Duration Debt Index",
    "NIFTY Ultra Short Duration Debt Index":  "Nifty Ultra Short Duration Debt Index",
    "NIFTY Medium Duration Debt Index":       "Nifty Medium Duration Debt Index",
    "NIFTY Medium to Long Duration Debt Index": "Nifty Medium to Long Duration Debt Index",
    "NIFTY Long Duration Debt Index":         "Nifty Long Duration Debt Index",
    "NIFTY Floater Long Term Debt Index":     "Nifty Floater Long Term Debt Index",

    # Hybrid / strategy indices
    "NIFTY 50 Hybrid Composite Debt 15:85 Index": "Nifty 50 Hybrid Composite Debt 15:85 Index",
    "NIFTY 50 Hybrid Composite Debt 50:50 Index": "Nifty 50 Hybrid Composite Debt 50:50 Index",
    "NIFTY 50 Hybrid Composite Debt 65:35 Index": "Nifty 50 Hybrid Composite Debt 65:35 Index",
    "NIFTY Equity Savings Index":             "Nifty Equity Savings Index",
    "NIFTY 50 Arbitrage Index":               "Nifty 50 Arbitrage Index",
}

# ── BSE index name mapping ────────────────────────────────────────────────────
# Maps our DB benchmark name → BSE index code / name used by bseindia.com API.

BSE_INDEX_MAP: dict[str, str] = {
    "S&P BSE 100 TRI":                     "S&P BSE 100",
    "S&P BSE 500 TRI":                     "S&P BSE 500",
    "S&P BSE 250 Large MidCap TRI":        "S&P BSE LargeMidCap",
    "S&P BSE 250 SmallCap TRI":            "S&P BSE SmallCap",
    "S&P BSE Midcap 150 TRI":              "S&P BSE Midcap",
    "S&P BSE MNC TRI":                     "S&P BSE MNC",
    "S&P BSE Auto TRI":                    "S&P BSE Auto",
    "S&P BSE Bankex TRI":                  "S&P BSE Bankex",
    "S&P BSE Financial Services TRI":      "S&P BSE Finance",
    "S&P BSE FMCG TRI":                    "S&P BSE FMCG",
    "S&P BSE Healthcare TRI":              "S&P BSE Healthcare",
    "S&P BSE IT TRI":                      "S&P BSE IT",
    "S&P BSE India Infrastructure TRI":    "S&P BSE India Infrastructure",
    "S&P BSE India Manufacturing TRI":     "S&P BSE India Manufacturing",
    "S&P BSE Energy TRI":                  "S&P BSE Energy",
    "S&P BSE Metal TRI":                   "S&P BSE Metal",
    "S&P BSE PSU TRI":                     "S&P BSE PSU",
    "S&P BSE Commodities TRI":             "S&P BSE Commodities",
    "S&P BSE Transport TRI":               "S&P BSE Transport",
    "S&P BSE Dividend Stability TRI":      "S&P BSE Dividend Stability",
    "S&P BSE India Consumer Discretionary TRI": "S&P BSE Consumer Discretionary",
    "S&P BSE 100 ESG TRI":                 "S&P BSE 100 ESG",
    "S&P BSE India Defence TRI":           "S&P BSE India Defence",
    "S&P BSE Agri Index TRI":              "S&P BSE Agri",
    "S&P BSE Export TRI":                  "S&P BSE Export",
}

# CRISIL indices — no public API; download manually from:
# https://www.crisil.com/en/home/our-businesses/crisil-indices.html
# Save each as a CSV with columns: Date, Close
# Then copy to benchmark_csvs/ and run load_benchmarks.py
CRISIL_MANUAL = [
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
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_filename(name: str) -> str:
    """Convert an index name to a safe filename."""
    return name.replace("/", "-").replace(":", "-").replace(" ", "_").replace("&", "and") + ".csv"


def _date_chunks(start: date, end: date, chunk_days: int):
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def _write_csv(path: Path, rows: list[tuple[str, float]], append: bool = False) -> int:
    """Write (date_str, close_value) rows to CSV. Returns rows written."""
    mode = "a" if append else "w"
    with open(path, mode, newline="") as f:
        w = csv.writer(f)
        if not append:
            w.writerow(["Date", "Close"])
        w.writerows(rows)
    return len(rows)


# ── niftyindices.com downloader ───────────────────────────────────────────────
# niftyindices.com uses ASP.NET WebMethods.  The session must be warmed up by:
#   1. GET the homepage  (gets ASP.NET_SessionId cookie)
#   2. GET the specific index page  (registers the page context server-side)
# Only then will the Backpage.aspx POST return data instead of an empty body.

NIFTY_BASE    = "https://niftyindices.com"
NIFTY_URL     = "https://niftyindices.com/Backpage.aspx/getHistoricaldatatable"

# Slug used in the index page URL — only needed for session warmup.
# Maps api_name → URL path fragment on niftyindices.com.
NIFTY_PAGE_SLUG: dict[str, str] = {
    "Nifty 100 Total Returns Index":                          "equity/broad-based-indices/NIFTY-100",
    "Nifty 500 Total Returns Index":                          "equity/broad-based-indices/NIFTY-500",
    "Nifty 500 Multicap 50:25:25 Total Returns Index":        "equity/broad-based-indices/NIFTY-500-Multicap-50-25-25",
    "Nifty Large Midcap 250 Total Returns Index":             "equity/broad-based-indices/NIFTY-LargeMidcap-250",
    "Nifty Midcap 150 Total Returns Index":                   "equity/broad-based-indices/NIFTY-Midcap-150",
    "Nifty Smallcap 250 Total Returns Index":                 "equity/broad-based-indices/NIFTY-Smallcap-250",
    "Nifty Dividend Opportunities 50 Total Returns Index":    "equity/strategy-indices/NIFTY-Dividend-Opportunities-50",
    "Nifty Auto Total Returns Index":                         "equity/sectoral-indices/NIFTY-Auto",
    "Nifty Bank Total Returns Index":                         "equity/sectoral-indices/NIFTY-Bank",
    "Nifty Commodities Total Returns Index":                  "equity/sectoral-indices/NIFTY-Commodities",
    "Nifty Energy Total Returns Index":                       "equity/sectoral-indices/NIFTY-Energy",
    "Nifty FMCG Total Returns Index":                         "equity/sectoral-indices/NIFTY-FMCG",
    "Nifty Financial Services Total Returns Index":           "equity/sectoral-indices/NIFTY-Financial-Services",
    "Nifty Healthcare Total Returns Index":                   "equity/sectoral-indices/NIFTY-Healthcare",
    "Nifty IT Total Returns Index":                           "equity/sectoral-indices/NIFTY-IT",
    "Nifty India Consumption Total Returns Index":            "equity/sectoral-indices/NIFTY-India-Consumption",
    "Nifty India Defence Total Returns Index":                "equity/sectoral-indices/NIFTY-India-Defence",
    "Nifty India Manufacturing Total Returns Index":          "equity/sectoral-indices/NIFTY-India-Manufacturing",
    "Nifty India Rural Total Returns Index":                  "equity/sectoral-indices/NIFTY-India-Rural",
    "Nifty India Transport & Logistics Total Returns Index":  "equity/sectoral-indices/NIFTY-India-Transport-Logistics",
    "Nifty Infrastructure Total Returns Index":               "equity/sectoral-indices/NIFTY-Infrastructure",
    "Nifty Pharma Total Returns Index":                       "equity/sectoral-indices/NIFTY-Pharma",
    "Nifty PSE Total Returns Index":                          "equity/sectoral-indices/NIFTY-PSE",
    "Nifty100 ESG Sector Leaders Total Returns Index":        "equity/strategy-indices/NIFTY100-ESG-Sector-Leaders",
    "Nifty Export and Logistic Total Returns Index":          "equity/sectoral-indices/NIFTY-India-Export-and-Logistic",
    "Nifty Composite Debt Index":                             "debt/nifty-composite-debt-index",
    "Nifty Corporate Bond Index":                             "debt/nifty-corporate-bond-index",
    "Nifty Credit Risk Bond Index":                           "debt/nifty-credit-risk-bond-index",
    "Nifty Banking and PSU Debt Index":                       "debt/nifty-banking-and-psu-debt-index",
    "Nifty All Duration G-Sec Index":                         "debt/nifty-all-duration-g-sec-index",
    "Nifty 10yr Benchmark G-Sec Index":                       "debt/nifty-10yr-benchmark-g-sec-index",
    "Nifty Liquid Index A-I":                                 "debt/nifty-liquid-index",
    "Nifty 1D Rate Index":                                    "debt/nifty-1d-rate-index",
    "Nifty Money Market Index":                               "debt/nifty-money-market-index",
    "Nifty Short Duration Debt Index":                        "debt/nifty-short-duration-debt-index",
    "Nifty Low Duration Debt Index":                          "debt/nifty-low-duration-debt-index",
    "Nifty Ultra Short Duration Debt Index":                  "debt/nifty-ultra-short-duration-debt-index",
    "Nifty Medium Duration Debt Index":                       "debt/nifty-medium-duration-debt-index",
    "Nifty Medium to Long Duration Debt Index":               "debt/nifty-medium-to-long-duration-debt-index",
    "Nifty Long Duration Debt Index":                         "debt/nifty-long-duration-debt-index",
    "Nifty Floater Long Term Debt Index":                     "debt/nifty-floater-long-term-debt-index",
    "Nifty 50 Hybrid Composite Debt 15:85 Index":             "hybrid/nifty-50-hybrid-composite-debt-15-85-index",
    "Nifty 50 Hybrid Composite Debt 50:50 Index":             "hybrid/nifty-50-hybrid-composite-debt-50-50-index",
    "Nifty 50 Hybrid Composite Debt 65:35 Index":             "hybrid/nifty-50-hybrid-composite-debt-65-35-index",
    "Nifty Equity Savings Index":                             "hybrid/nifty-equity-savings-index",
    "Nifty 50 Arbitrage Index":                               "hybrid/nifty-50-arbitrage-index",
}


def _nifty_session(api_name: str, cookie: str | None = None,
                   debug: bool = False) -> requests.Session:
    """
    Build a requests.Session for niftyindices.com.

    niftyindices.com requires a logged-in session for the historical data API.

    Option A (recommended) — pass your browser session cookie:
        Run with:  --nifty-cookie "ASP.NET_SessionId=abc123; .ASPXAUTH=xyz..."
        How to get it:
          1. Go to https://niftyindices.com and log in (free registration)
          2. Open DevTools → Network → any request → Headers → Cookie
          3. Copy the full Cookie header value and pass it via --nifty-cookie

    Option B — the script tries anonymous warmup (usually fails, kept as fallback).
    """
    s = requests.Session()
    s.headers.update({
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })

    if cookie:
        # Inject the browser session cookie directly — no warmup needed
        s.headers.update({"Cookie": cookie})
        if debug:
            print(f"    [debug] using provided cookie: {cookie[:60]}...")
        return s

    # Fallback: anonymous warmup (niftyindices.com usually rejects this)
    print("  [warn] No --nifty-cookie provided; attempting anonymous session "
          "(likely to fail — see --help for how to get your cookie).")
    r1 = s.get(NIFTY_BASE, timeout=20)
    if debug:
        print(f"    [debug] homepage status={r1.status_code} cookies={dict(s.cookies)}")
    time.sleep(1)

    slug = NIFTY_PAGE_SLUG.get(api_name)
    if slug:
        r2 = s.get(f"{NIFTY_BASE}/indices/{slug}", timeout=20)
        if debug:
            print(f"    [debug] index page status={r2.status_code}")
        time.sleep(1)

    return s


def _fetch_nifty_chunk(session: requests.Session, api_name: str,
                        start: date, end: date,
                        debug: bool = False) -> list[tuple[str, float]]:
    """Fetch one chunk from niftyindices.com. Returns [(iso_date, close), ...]."""
    from datetime import datetime

    payload = {
        "name":      api_name,
        "startDate": start.strftime("%d-%b-%Y"),   # "01-Jan-2006"
        "endDate":   end.strftime("%d-%b-%Y"),
    }
    slug    = NIFTY_PAGE_SLUG.get(api_name, "")
    referer = f"{NIFTY_BASE}/indices/{slug}" if slug else NIFTY_BASE

    resp = session.post(
        NIFTY_URL,
        json=payload,
        timeout=30,
        headers={
            "Content-Type":     "application/json; charset=UTF-8",
            "Accept":           "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer":          referer,
        },
    )

    if debug:
        print(f"    [debug] POST status={resp.status_code} size={len(resp.text)} "
              f"body_preview={resp.text[:120]!r}")

    resp.raise_for_status()

    if len(resp.text.strip()) < MIN_BYTES:
        if debug:
            print("    [debug] empty response body")
        return []

    outer     = resp.json()
    inner_str = outer.get("d", "[]")
    rows_raw  = json.loads(inner_str)

    results = []
    for row in rows_raw:
        raw_date  = row.get("HistoricalDate", "").strip()
        raw_close = row.get("CLOSE", "").replace(",", "").strip()
        if not raw_date or not raw_close or raw_close in ("-", ""):
            continue
        try:
            raw_date = raw_date.replace("-", " ")
            d = datetime.strptime(raw_date, "%d %b %Y").date()
            results.append((d.isoformat(), float(raw_close)))
        except (ValueError, TypeError):
            continue

    return results


def download_nifty_index(db_name: str, api_name: str,
                          start: date, end: date,
                          out_dir: Path, cookie: str | None = None,
                          debug: bool = False) -> None:
    fname  = out_dir / _safe_filename(db_name)
    append = False

    if fname.exists():
        with open(fname) as f:
            rows = list(csv.reader(f))
        if len(rows) > 1:
            last_date = date.fromisoformat(rows[-1][0])
            if last_date >= end:
                print(f"  [skip]  {db_name}  (up to date: {last_date})")
                return
            start  = last_date + timedelta(days=1)
            append = True
            print(f"  [resume] {db_name}  from {start}")

    print(f"  [nifty] {db_name}")
    session = _nifty_session(api_name, cookie=cookie, debug=debug)
    total   = 0

    for chunk_start, chunk_end in _date_chunks(start, end, CHUNK_DAYS):
        try:
            rows = _fetch_nifty_chunk(session, api_name, chunk_start, chunk_end, debug=debug)
            if rows:
                total += _write_csv(fname, rows, append=append or total > 0)
                print(f"    {chunk_start} → {chunk_end}: {len(rows)} rows")
            else:
                print(f"    {chunk_start} → {chunk_end}: no data (index may not exist yet)")
        except Exception as e:
            print(f"    {chunk_start} → {chunk_end}: ERROR — {e}")
            if debug:
                import traceback; traceback.print_exc()
        time.sleep(DELAY_SECS)

    print(f"  → {total} rows saved to {fname.name}")


# ── bseindia.com downloader ───────────────────────────────────────────────────

BSE_URL     = "https://api.bseindia.com/BseIndiaAPI/api/GetIndicesHistData/w"
BSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer":    "https://www.bseindia.com/",
    "Origin":     "https://www.bseindia.com",
    "Accept":     "application/json, text/plain, */*",
}


def _bse_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(BSE_HEADERS)
    s.get("https://www.bseindia.com", timeout=20)
    time.sleep(1)
    return s


def _fetch_bse_chunk(session: requests.Session, api_name: str,
                      start: date, end: date) -> list[tuple[str, float]]:
    """Fetch one chunk from bseindia.com API."""
    params = {
        "Indice":   api_name,
        "startDt":  start.strftime("%Y%m%d"),   # e.g. "20060101"
        "endDt":    end.strftime("%Y%m%d"),
    }
    resp = session.get(BSE_URL, params=params, timeout=30)
    resp.raise_for_status()

    if len(resp.text) < MIN_BYTES:
        return []

    try:
        data = resp.json()
    except Exception:
        return []

    # BSE API returns a list of dicts with 'Date' and 'close' / 'Close' fields
    results = []
    records = data if isinstance(data, list) else data.get("Table", data.get("data", []))

    for row in records:
        # BSE API may return different key cases; try both
        raw_date  = row.get("Date") or row.get("date") or row.get("DT") or ""
        raw_close = row.get("Close") or row.get("close") or row.get("CLOSE") or ""
        if not raw_date or not raw_close:
            continue
        try:
            raw_close_f = float(str(raw_close).replace(",", ""))
            # BSE dates come in various formats: "2024-01-01", "01/01/2024"
            raw_date_s = str(raw_date).strip()[:10]
            from datetime import datetime
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
                try:
                    d = datetime.strptime(raw_date_s, fmt).date()
                    break
                except ValueError:
                    continue
            else:
                continue
            results.append((d.isoformat(), raw_close_f))
        except (ValueError, TypeError):
            continue

    return results


def download_bse_index(db_name: str, api_name: str,
                        start: date, end: date, out_dir: Path) -> None:
    fname = out_dir / _safe_filename(db_name)

    if fname.exists():
        with open(fname) as f:
            rows = list(csv.reader(f))
        if len(rows) > 1:
            last_date = date.fromisoformat(rows[-1][0])
            if last_date >= end:
                print(f"  [skip]  {db_name}  (up to date: {last_date})")
                return
            start = last_date + timedelta(days=1)
            print(f"  [resume] {db_name}  from {start}")
            append = True
        else:
            append = False
    else:
        append = False

    print(f"  [bse]   {db_name}")
    session = _bse_session()
    total   = 0

    for chunk_start, chunk_end in _date_chunks(start, end, CHUNK_DAYS):
        try:
            rows = _fetch_bse_chunk(session, api_name, chunk_start, chunk_end)
            if rows:
                total += _write_csv(fname, rows, append=append or total > 0)
                print(f"    {chunk_start} → {chunk_end}: {len(rows)} rows")
            else:
                print(f"    {chunk_start} → {chunk_end}: no data")
        except Exception as e:
            print(f"    {chunk_start} → {chunk_end}: ERROR — {e}")
        time.sleep(DELAY_SECS)

    print(f"  → {total} rows saved to {fname.name}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download benchmark index data to CSV (run on local machine)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--from",    dest="from_date", default=str(DEFAULT_START),
                        help="Start date YYYY-MM-DD")
    parser.add_argument("--to",      dest="to_date",
                        default=date.today().isoformat(),
                        help="End date YYYY-MM-DD")
    parser.add_argument("--out-dir", dest="out_dir", default=str(OUT_DIR),
                        help="Output directory for CSV files")
    parser.add_argument("--source",  choices=["nifty", "bse", "all"], default="all",
                        help="Which source to download from")
    parser.add_argument("--index",   dest="index_filter", default=None,
                        help="Download only this specific DB index name")
    parser.add_argument("--debug",        action="store_true",
                        help="Print raw HTTP responses for troubleshooting")
    parser.add_argument("--nifty-cookie", dest="nifty_cookie", default=None,
                        help="Full Cookie header from a logged-in niftyindices.com browser "
                             "session. Get it from DevTools → Network → any request → "
                             "Headers → Cookie. Wrap in quotes: "
                             "--nifty-cookie \"ASP.NET_SessionId=abc; .ASPXAUTH=xyz\"")
    args = parser.parse_args()

    start   = date.fromisoformat(args.from_date)
    end     = date.fromisoformat(args.to_date)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Downloading benchmark data: {start} → {end}")
    print(f"Output dir: {out_dir.resolve()}\n")

    # ── NIFTY indices ─────────────────────────────────────────────────────────
    if args.source in ("nifty", "all"):
        print("=== NIFTY indices (niftyindices.com) ===")
        if not args.nifty_cookie:
            print("  [hint] No --nifty-cookie supplied. To get one:")
            print("    1. Go to https://niftyindices.com and create a free account")
            print("    2. Log in, open DevTools (F12) → Network tab")
            print("    3. Click any page, find any request, copy the Cookie header value")
            print("    4. Re-run with:  --nifty-cookie \"<paste cookie here>\"\n")
        for db_name, api_name in NIFTY_INDEX_MAP.items():
            if args.index_filter and db_name != args.index_filter:
                continue
            download_nifty_index(db_name, api_name, start, end, out_dir,
                                 cookie=args.nifty_cookie, debug=args.debug)

    # ── BSE indices ───────────────────────────────────────────────────────────
    if args.source in ("bse", "all"):
        print("\n=== S&P BSE indices (bseindia.com) ===")
        for db_name, api_name in BSE_INDEX_MAP.items():
            if args.index_filter and db_name != args.index_filter:
                continue
            download_bse_index(db_name, api_name, start, end, out_dir)

    # ── CRISIL (manual) ───────────────────────────────────────────────────────
    if args.source == "all" and not args.index_filter:
        print("\n=== CRISIL indices — MANUAL DOWNLOAD REQUIRED ===")
        print("Download from: https://www.crisil.com/en/home/our-businesses/crisil-indices.html")
        print("Save each index as a CSV with columns: Date, Close")
        print("Place files in:", out_dir.resolve())
        print("Expected filenames:")
        for name in CRISIL_MANUAL:
            print(f"  {_safe_filename(name)}")

    print("\nDone.")
    print(f"\nNext step: copy {out_dir}/ to the server and run:")
    print("  python load_benchmarks.py --csv-dir benchmark_csvs/")


if __name__ == "__main__":
    main()
