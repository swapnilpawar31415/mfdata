#!/usr/bin/env python3
"""
Populate schemes.benchmark and schemes.benchmark_2 using SEBI-mandated
tier-1 benchmark indices.

Convention
----------
benchmark   — primary index: NIFTY/NSE where one exists, BSE where NIFTY
              does not cover the sector (e.g. Telecom, MNC, Media).
benchmark_2 — secondary index: BSE/CRISIL alternative, NULL when no
              meaningful second index exists.

For Sectoral/Thematic schemes the benchmark cannot be inferred from the
category alone; the script uses a keyword-priority list against the scheme
name to assign the most appropriate sector index.

Sources
-------
  • AMFI tier-1 benchmark list  https://www.amfiindia.com/otherdata/listofbenchmarkindices
  • SEBI Master Circular on Mutual Funds, Jun 2024

Usage
-----
    python fetch_benchmarks.py              # update mf.db in-place
    python fetch_benchmarks.py --db /path/to/mf.db
    python fetch_benchmarks.py --dry-run    # show counts without writing
"""

import argparse

from db import DB_PATH, get_connection

# ── Category-level mapping ────────────────────────────────────────────────────
# Each value is (benchmark, benchmark_2).
# None means "no universal benchmark for this category" (handled separately
# by SECTORAL_RULES for Sectoral/Thematic; left NULL for Index/ETF/FoF).

CATEGORY_BENCHMARKS: dict[str, tuple[str | None, str | None]] = {

    # ── Equity ────────────────────────────────────────────────────────────────
    "Equity Scheme - Large Cap Fund":
        ("NIFTY 100 TRI",                         "S&P BSE 100 TRI"),
    "Equity Scheme - Large & Mid Cap Fund":
        ("NIFTY Large Midcap 250 TRI",            "S&P BSE 250 Large MidCap TRI"),
    "Equity Scheme - Mid Cap Fund":
        ("NIFTY Midcap 150 TRI",                  "S&P BSE Midcap 150 TRI"),
    "Equity Scheme - Small Cap Fund":
        ("NIFTY Smallcap 250 TRI",                "S&P BSE 250 SmallCap TRI"),
    "Equity Scheme - Multi Cap Fund":
        ("NIFTY 500 Multicap 50:25:25 TRI",       "S&P BSE 500 TRI"),
    "Equity Scheme - Flexi Cap Fund":
        ("NIFTY 500 TRI",                         "S&P BSE 500 TRI"),
    "Equity Scheme - ELSS":
        ("NIFTY 500 TRI",                         "S&P BSE 500 TRI"),
    "Equity Scheme - Value Fund":
        ("NIFTY 500 TRI",                         "S&P BSE 500 TRI"),
    "Equity Scheme - Contra Fund":
        ("NIFTY 500 TRI",                         "S&P BSE 500 TRI"),
    "Equity Scheme - Focused Fund":
        ("NIFTY 500 TRI",                         "S&P BSE 500 TRI"),
    "Equity Scheme - Dividend Yield Fund":
        ("NIFTY Dividend Opportunities 50 TRI",   "S&P BSE Dividend Stability TRI"),
    "Equity Scheme - Sectoral/ Thematic":
        (None, None),   # resolved per-scheme via SECTORAL_RULES below

    # Legacy pre-2018 equity names
    "Growth":   ("NIFTY 500 TRI", "S&P BSE 500 TRI"),
    "ELSS":     ("NIFTY 500 TRI", "S&P BSE 500 TRI"),

    # ── Debt ──────────────────────────────────────────────────────────────────
    "Debt Scheme - Overnight Fund":
        ("NIFTY 1D Rate Index",                       "CRISIL Overnight Index"),
    "Debt Scheme - Liquid Fund":
        ("NIFTY Liquid Index A-I",                    "CRISIL Liquid Fund Index"),
    "Debt Scheme - Ultra Short Duration Fund":
        ("NIFTY Ultra Short Duration Debt Index",     "CRISIL Ultra Short Term Debt Index"),
    "Debt Scheme - Low Duration Fund":
        ("NIFTY Low Duration Debt Index",             "CRISIL Low Duration Debt Index"),
    "Debt Scheme - Money Market Fund":
        ("NIFTY Money Market Index",                  "CRISIL Money Market Index"),
    "Debt Scheme - Short Duration Fund":
        ("NIFTY Short Duration Debt Index",           "CRISIL Short Duration Debt Index"),
    "Debt Scheme - Medium Duration Fund":
        ("NIFTY Medium Duration Debt Index",          "CRISIL Medium Term Debt Index"),
    "Debt Scheme - Medium to Long Duration Fund":
        ("NIFTY Medium to Long Duration Debt Index",  "CRISIL Composite Bond Fund Index"),
    "Debt Scheme - Long Duration Fund":
        ("NIFTY Long Duration Debt Index",            "CRISIL Dynamic Gilt Index"),
    "Debt Scheme - Dynamic Bond":
        ("NIFTY Composite Debt Index",                "CRISIL Composite Bond Fund Index"),
    "Debt Scheme - Corporate Bond Fund":
        ("NIFTY Corporate Bond Index",                "CRISIL Corporate Bond Composite Index"),
    "Debt Scheme - Credit Risk Fund":
        ("NIFTY Credit Risk Bond Index",              "CRISIL Short Term Credit Risk Index"),
    "Debt Scheme - Banking and PSU Fund":
        ("NIFTY Banking and PSU Debt Index",          "CRISIL Banking and PSU Debt Index"),
    "Debt Scheme - Gilt Fund":
        ("NIFTY All Duration G-Sec Index",            "CRISIL Dynamic Gilt Index"),
    "Debt Scheme - Gilt Fund with 10 year constant duration":
        ("NIFTY 10yr Benchmark G-Sec Index",          "CRISIL 10 Year Gilt Index"),
    "Debt Scheme - Floater Fund":
        ("NIFTY Floater Long Term Debt Index",        "CRISIL Liquid Fund Index"),

    # Legacy pre-2018 debt names
    "Income":        ("NIFTY Composite Debt Index",        "CRISIL Composite Bond Fund Index"),
    "Liquid":        ("NIFTY Liquid Index A-I",            "CRISIL Liquid Fund Index"),
    "Gilt":          ("NIFTY All Duration G-Sec Index",    "CRISIL Dynamic Gilt Index"),
    "Floating Rate": ("NIFTY Floater Long Term Debt Index","CRISIL Liquid Fund Index"),
    "Money Market":  ("NIFTY Money Market Index",          "CRISIL Money Market Index"),

    # ── Hybrid ────────────────────────────────────────────────────────────────
    "Hybrid Scheme - Conservative Hybrid Fund":
        ("NIFTY 50 Hybrid Composite Debt 15:85 Index", "CRISIL Hybrid 85+15 Conservative Index"),
    "Hybrid Scheme - Balanced Hybrid Fund":
        ("NIFTY 50 Hybrid Composite Debt 50:50 Index", "CRISIL Hybrid 50+50 Moderate Index"),
    "Hybrid Scheme - Aggressive Hybrid Fund":
        ("NIFTY 50 Hybrid Composite Debt 65:35 Index", "CRISIL Hybrid 35+65 Aggressive Index"),
    "Hybrid Scheme - Dynamic Asset Allocation or Balanced Advantage":
        ("NIFTY 50 Hybrid Composite Debt 65:35 Index", "CRISIL Hybrid 35+65 Aggressive Index"),
    "Hybrid Scheme - Multi Asset Allocation":
        ("NIFTY 50 Hybrid Composite Debt 65:35 Index", "CRISIL Hybrid 35+65 Aggressive Index"),
    "Hybrid Scheme - Equity Savings":
        ("NIFTY Equity Savings Index",                 "CRISIL Hybrid 25+75 Conservative Index"),
    "Hybrid Scheme - Arbitrage Fund":
        ("NIFTY 50 Arbitrage Index",                   "CRISIL Liquid Fund Index"),

    # Legacy pre-2018 hybrid name
    "Balanced": ("NIFTY 50 Hybrid Composite Debt 50:50 Index", "CRISIL Hybrid 50+50 Moderate Index"),

    # ── Solution Oriented ─────────────────────────────────────────────────────
    "Solution Oriented Scheme - Retirement Fund":
        ("NIFTY 50 Hybrid Composite Debt 50:50 Index", "CRISIL Hybrid 50+50 Moderate Index"),
    "Solution Oriented Scheme - Children\xe2\x80\x99s Fund":
        ("NIFTY 50 Hybrid Composite Debt 50:50 Index", "CRISIL Hybrid 50+50 Moderate Index"),

    # ── Index / ETF / FoF — benchmark is fund-specific, left NULL ─────────────
    "Other Scheme - Index Funds":   (None, None),
    "Other Scheme - Other  ETFs":   (None, None),
    "Other Scheme - FoF Domestic":  (None, None),
    "Other Scheme - FoF Overseas":  (None, None),
    "Other Scheme - Gold ETF":      ("Domestic Price of Gold", None),
    "Other ETFs":                   (None, None),
    "GOLD ETFs":                    ("Domestic Price of Gold", None),
    "Fund of Funds - Domestic":     (None, None),
    "Fund of Funds - Overseas":     (None, None),
}

# ── Sectoral / Thematic keyword rules ────────────────────────────────────────
# Applied only to  category = 'Equity Scheme - Sectoral/ Thematic'.
# Rules are evaluated in order; the first matching rule wins.
# Each rule: (keywords_any_of, benchmark, benchmark_2)
#   keywords_any_of — list of lowercase substrings; matches if ANY is found
#                     in the lowercased scheme name.
# Where no NIFTY index exists for a sector (Telecom, MNC, Media), benchmark
# is set to the BSE index and benchmark_2 is None.

SECTORAL_RULES: list[tuple[list[str], str | None, str | None]] = [
    # Most specific patterns first to avoid false matches
    (["financial services", "banking and financial", "bank & financial",
      "banking & financial", "banking and fin"],
     "NIFTY Financial Services TRI",          "S&P BSE Financial Services TRI"),

    (["banking"],
     "NIFTY Bank TRI",                        "S&P BSE Bankex TRI"),

    (["pharma"],
     "NIFTY Pharma TRI",                      "S&P BSE Healthcare TRI"),

    (["health"],
     "NIFTY Healthcare TRI",                  "S&P BSE Healthcare TRI"),

    (["information technology"],
     "NIFTY IT TRI",                          "S&P BSE IT TRI"),

    (["technology", "digital india", "digital"],
     "NIFTY IT TRI",                          "S&P BSE IT TRI"),

    (["infrastructure"],
     "NIFTY Infrastructure TRI",              "S&P BSE India Infrastructure TRI"),

    (["fmcg"],
     "NIFTY FMCG TRI",                        "S&P BSE FMCG TRI"),

    (["consumption"],
     "NIFTY India Consumption TRI",           "S&P BSE India Consumer Discretionary TRI"),

    (["energy"],
     "NIFTY Energy TRI",                      "S&P BSE Energy TRI"),

    (["auto"],
     "NIFTY Auto TRI",                        "S&P BSE Auto TRI"),

    (["psu", "public sector"],
     "NIFTY PSE TRI",                         "S&P BSE PSU TRI"),

    # No NIFTY equivalent — BSE is primary
    (["mnc"],
     "S&P BSE MNC TRI",                       None),

    # No NIFTY equivalent — BSE is primary
    (["telecom"],
     "S&P BSE Telecom TRI",                   None),

    (["manufactur"],
     "NIFTY India Manufacturing TRI",         "S&P BSE India Manufacturing TRI"),

    (["transport", "logistic"],
     "NIFTY India Transport & Logistics TRI", "S&P BSE Transport TRI"),

    # No NIFTY equivalent — BSE is primary
    (["media", "entertainment"],
     "S&P BSE Media & Entertainment TRI",     None),

    (["metal", "mining"],
     "NIFTY Metal TRI",                       "S&P BSE Metal TRI"),

    (["commodit", "natural resource"],
     "NIFTY Commodities TRI",                 "S&P BSE Commodities TRI"),

    (["esg"],
     "NIFTY 100 ESG TRI",                     "S&P BSE 100 ESG TRI"),

    (["defense", "defence"],
     "NIFTY India Defence TRI",               "S&P BSE India Defence TRI"),

    (["realt", "real estate"],
     "NIFTY Realty TRI",                      "S&P BSE Realty TRI"),

    (["rural", "agri"],
     "NIFTY India Rural Index TRI",           "S&P BSE Agri Index TRI"),

    (["export"],
     "NIFTY Export & Logistic TRI",           "S&P BSE Export TRI"),

    # International/overseas — too varied to assign a single index
    (["international", "global", "overseas"],
     None, None),

    # Broad fallback for quant, business cycle, opportunity, thematic, etc.
    (["quant", "business cycle", "opportunit", "thematic",
      "multi-sector", "multi sector", "conglomerat"],
     "NIFTY 500 TRI",                         "S&P BSE 500 TRI"),
]


def _match_sectoral(name: str) -> tuple[str | None, str | None]:
    """Return (benchmark, benchmark_2) for a sectoral/thematic scheme by keyword."""
    name_lower = name.lower()
    for keywords, bm, bm2 in SECTORAL_RULES:
        if any(kw in name_lower for kw in keywords):
            return bm, bm2
    # No rule matched — use broad market as a safe fallback
    return "NIFTY 500 TRI", "S&P BSE 500 TRI"


# ── Main ─────────────────────────────────────────────────────────────────────

def run(db_path=None, dry_run: bool = False) -> None:
    conn  = get_connection(db_path)
    cur   = conn.cursor()

    # ── 1. Category-level updates (all non-sectoral categories) ──────────────
    cur.execute("SELECT DISTINCT category FROM schemes WHERE category IS NOT NULL")
    db_categories = {row[0] for row in cur.fetchall()}

    mapped   = {c: CATEGORY_BENCHMARKS[c] for c in db_categories if c in CATEGORY_BENCHMARKS}
    unmapped = db_categories - set(CATEGORY_BENCHMARKS)

    print(f"Categories in DB          : {len(db_categories)}")
    print(f"  Mapped (have rule)      : {len(mapped)}")
    print(f"  Unmapped (no rule)      : {len(unmapped)}")
    if unmapped:
        print("\nUnmapped categories (will be left NULL):")
        for c in sorted(unmapped):
            print(f"  • {c}")

    if not dry_run:
        with conn:
            for category, (bm, bm2) in mapped.items():
                if category == "Equity Scheme - Sectoral/ Thematic":
                    continue   # handled below per-scheme
                cur.execute(
                    "UPDATE schemes SET benchmark = ?, benchmark_2 = ? WHERE category = ?",
                    (bm, bm2, category),
                )

    # ── 2. Per-scheme keyword matching for Sectoral / Thematic ───────────────
    cur.execute(
        "SELECT scheme_code, scheme_name FROM schemes "
        "WHERE category = 'Equity Scheme - Sectoral/ Thematic'"
    )
    sectoral = cur.fetchall()

    matched_sectoral   = 0
    fallback_sectoral  = 0
    unmatched_sectoral = 0

    updates = []
    for scheme_code, scheme_name in sectoral:
        bm, bm2 = _match_sectoral(scheme_name)
        if bm is None:
            unmatched_sectoral += 1
        elif bm in ("NIFTY 500 TRI", "S&P BSE 500 TRI"):
            fallback_sectoral += 1
        else:
            matched_sectoral += 1
        updates.append((bm, bm2, scheme_code))

    if not dry_run:
        with conn:
            cur.executemany(
                "UPDATE schemes SET benchmark = ?, benchmark_2 = ? WHERE scheme_code = ?",
                updates,
            )

    print(f"\nSectoral/Thematic schemes : {len(sectoral)}")
    print(f"  Sector-specific match   : {matched_sectoral}")
    print(f"  Broad fallback (NIFTY 500): {fallback_sectoral}")
    print(f"  No match (NULL)         : {unmatched_sectoral}")

    conn.close()

    if dry_run:
        print("\n[dry-run] No changes written.")
        return

    # ── 3. Summary ────────────────────────────────────────────────────────────
    conn2 = get_connection(db_path)
    cur2  = conn2.cursor()
    cur2.execute("SELECT COUNT(*) FROM schemes WHERE benchmark IS NOT NULL")
    with_bm = cur2.fetchone()[0]
    cur2.execute("SELECT COUNT(*) FROM schemes WHERE benchmark IS NULL")
    without_bm = cur2.fetchone()[0]
    cur2.execute("SELECT COUNT(*) FROM schemes WHERE benchmark_2 IS NOT NULL")
    with_bm2 = cur2.fetchone()[0]
    conn2.close()

    print(f"\nDB summary:")
    print(f"  Schemes with benchmark  : {with_bm:,}")
    print(f"  Schemes with benchmark_2: {with_bm2:,}")
    print(f"  Schemes without any     : {without_bm:,}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Set schemes.benchmark / benchmark_2 from SEBI tier-1 indices",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db",      default=str(DB_PATH), help="Path to SQLite database")
    parser.add_argument("--dry-run", action="store_true",  help="Show counts without writing")
    args = parser.parse_args()
    run(db_path=args.db, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
