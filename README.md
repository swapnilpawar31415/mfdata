# mfdata

SQLite-based analytics pipeline for Indian mutual funds — NAV history, benchmark returns, fund performance metrics, and star ratings for active and index funds.

---

## Database

Single file: `mf.db` (~5 GB). All scripts read/write this file via `db.py`.

| Table | Description |
|---|---|
| `schemes` | Fund identity: scheme_code, name, AMC, SEBI category, benchmarks, ISINs |
| `nav_history` | Daily NAV per scheme (36M+ rows, 2006–present) |
| `benchmark_returns` | Raw daily close values for benchmark indices (Yahoo Finance + NSE TRI via nsepython) |
| `benchmark_daily_returns` | Precomputed daily returns derived from `benchmark_returns` |
| `mf_daily_returns` | Daily returns for Growth/Regular funds, with excess return vs benchmark |
| `fund_performance` | 5-year trailing alpha, beta, IR, t-stat per active fund |
| `index_fund_performance` | 5-year trailing tracking error and tracking difference per index fund |
| `fund_ratings` | Star ratings 0–5 for all rated funds (active + index) within their peer group |

---

## Monthly Update

Run `update_mfdata.sh` after each month-end. It auto-detects the correct date range from the DB and runs all 10 pipeline steps in order, aborting on any failure.

```bash
# Auto-detect dates and run everything
./update_mfdata.sh

# Override end date (e.g. after March month-end)
./update_mfdata.sh --to 2026-03-31

# Preview dates without executing any steps
./update_mfdata.sh --dry-run
```

### What it runs (in order)

| Step | Script | What it does |
|---|---|---|
| 01 | `download_nav.py` | Download AMFI NAV history files into `./raw/` |
| 02 | `load_nav.py` | Parse and upsert NAV files into `nav_history` + `schemes` |
| 03 | `fetch_equity_benchmarks.py` | Yahoo Finance price indices → `benchmark_returns` |
| 04 | `fetch_nse_tri.py` | NSE TRI data via nsepython → `benchmark_returns` |
| 05 | `build_benchmark_daily_returns.py --append` | `benchmark_returns` → daily returns |
| 06 | `build_mf_daily_returns.py --append` | `nav_history` → daily returns for Growth/Regular funds |
| 07 | `build_excess_returns.py --append` | Fill `excess_return` = fund return − benchmark return |
| 08 | `build_fund_performance.py` | 5-year trailing alpha/beta/IR for active funds |
| 09 | `build_index_fund_performance.py` | 5-year trailing tracking error for index funds |
| 10 | `build_fund_ratings.py` | Unified 0–5 star ratings for active + index funds |

Output includes per-step elapsed time and a final DB row-count snapshot.

---

## Fund Ratings

### Active funds (`fund_performance` → `fund_ratings`)

Peer group: SEBI category (e.g. `Equity Scheme - Large Cap Fund`).

```
rating = (IR_percentile + t_stat_alpha_percentile) / 2 / 20
```

Rounded to 1 dp, capped at [0.0, 5.0]. A fund at the top of its peer group scores 100 → 5.0 stars.

### Index funds (`index_fund_performance` → `fund_ratings`)

Peer group: tracked index (e.g. `NIFTY 50 TRI`).

```
rating = tracking_error_percentile / 20
```

Lower tracking error = higher percentile = better rating. Both use the same `fund_ratings` table and 0–5 scale.

---

## Scripts Reference

| Script | Purpose |
|---|---|
| `db.py` | Centralised DDL + `get_connection()` |
| `download_nav.py` | Download AMFI NAV files for a date range |
| `load_nav.py` | Load NAV files into `nav_history` + `schemes` |
| `fetch_benchmarks.py` | Assign `schemes.benchmark` from category rules (active funds) |
| `fetch_equity_benchmarks.py` | Yahoo Finance → `benchmark_returns` (price return indices) |
| `fetch_nse_tri.py` | NSE TRI data via nsepython (no login required) → `benchmark_returns` |
| `load_benchmarks.py` | Load benchmark CSVs → `benchmark_returns` |
| `download_benchmarks.py` | **Run locally** — downloads NIFTY/BSE CSVs (requires niftyindices.com login cookie) |
| `build_benchmark_daily_returns.py` | `benchmark_returns` → `benchmark_daily_returns` (SQL LAG) |
| `build_mf_daily_returns.py` | `nav_history` → `mf_daily_returns` (Growth/Regular filter) |
| `build_excess_returns.py` | Fill `excess_return` in `mf_daily_returns` |
| `build_fund_performance.py` | 5-year trailing active fund metrics |
| `build_index_fund_performance.py` | 5-year trailing index fund tracking error |
| `build_fund_ratings.py` | Unified star ratings |

---

## Notes

- **Growth/Regular filter**: only schemes whose name contains `GROWTH`, does not contain `DIRECT`, and where `IDCW`/`DIVIDEND` does not appear after the last `GROWTH` occurrence.
- **Benchmark data gaps**: Most debt benchmarks (CRISIL, SDL) have no data. Sensex TRI is unavailable via nsepython (NSE API only). Funds in these categories are skipped from ratings.
- **Incremental append**: Steps 05–07 support `--append` mode, scanning only new rows. Full rebuilds can be triggered by omitting the flag.
- **Minimum peers**: `build_fund_ratings.py --min-peers N` (default 3) skips peer groups with fewer than N funds.
