#!/usr/bin/env bash
# update_mfdata.sh — Monthly pipeline to refresh all MF data and ratings.
#
# Steps (in order, each must succeed before the next runs):
#   1. download_nav.py         — Download AMFI NAV files
#   2. load_nav.py             — Load NAV files into nav_history / schemes
#   3. fetch_equity_benchmarks.py — Yahoo Finance price indices → benchmark_returns
#   4. fetch_nse_tri.py        — NSE TRI data (nsepython) → benchmark_returns
#   5. build_benchmark_daily_returns.py --append   — benchmark_returns → daily returns
#   6. build_mf_daily_returns.py --append          — nav_history → daily returns
#   7. build_excess_returns.py --append            — fill excess_return column
#   8. build_fund_performance.py                   — active fund alpha/IR metrics
#   9. build_index_fund_performance.py             — index fund tracking error
#  10. build_fund_ratings.py                       — unified star ratings
#
# Usage:
#   ./update_mfdata.sh                  # auto-detect from/to dates
#   ./update_mfdata.sh --to 2026-02-28  # override end date (default: today)
#   ./update_mfdata.sh --from 2026-02-01 --to 2026-02-28
#   ./update_mfdata.sh --dry-run        # print dates but don't execute steps

set -euo pipefail

# ── Colours ───────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

# ── Defaults ──────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TO_DATE="$(date +%Y-%m-%d)"
FROM_DATE=""
DRY_RUN=false

# ── Arg parsing ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --to)       TO_DATE="$2";   shift 2 ;;
        --from)     FROM_DATE="$2"; shift 2 ;;
        --dry-run)  DRY_RUN=true;   shift   ;;
        -h|--help)
            sed -n '2,25p' "$0" | sed 's/^# //'
            exit 0 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

cd "$SCRIPT_DIR"

# ── DB query helper ───────────────────────────────────────────────────────────
db_query() {
    python3 -c "
import sqlite3, sys
conn = sqlite3.connect('mf.db')
row = conn.execute(sys.argv[1]).fetchone()
print(row[0] if row and row[0] is not None else '')
conn.close()
" "$1"
}

# ── Auto-detect FROM_DATE for NAV download ────────────────────────────────────
if [[ -z "$FROM_DATE" ]]; then
    LAST_NAV=$(db_query "SELECT MAX(nav_date) FROM nav_history")
    if [[ -n "$LAST_NAV" ]]; then
        # Add 1 day in Python for portability
        FROM_DATE=$(python3 -c "
from datetime import date, timedelta
d = date.fromisoformat('$LAST_NAV') + timedelta(days=1)
print(d.isoformat())
")
    else
        FROM_DATE="2006-01-01"
    fi
fi

# Auto-detect last benchmark date for Yahoo Finance fetch
LAST_BM=$(db_query "SELECT MAX(index_date) FROM benchmark_returns WHERE source != 'nsepython'")
BM_FROM=""
if [[ -n "$LAST_BM" ]]; then
    BM_FROM=$(python3 -c "
from datetime import date, timedelta
d = date.fromisoformat('$LAST_BM') + timedelta(days=1)
print(d.isoformat())
")
fi

# ── Banner ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${BOLD}  MF Data Monthly Update Pipeline${RESET}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "  NAV window  : ${CYAN}${FROM_DATE}${RESET} → ${CYAN}${TO_DATE}${RESET}"
if [[ -n "$BM_FROM" ]]; then
    echo -e "  Equity BM   : ${CYAN}${BM_FROM}${RESET} → ${CYAN}${TO_DATE}${RESET}"
else
    echo -e "  Equity BM   : ${CYAN}full history${RESET}"
fi
if $DRY_RUN; then
    echo -e "  ${YELLOW}Mode: DRY RUN — no steps will execute${RESET}"
fi
echo ""

if [[ "$FROM_DATE" > "$TO_DATE" ]]; then
    echo -e "${YELLOW}NAV already up to ${TO_DATE}. Nothing to do.${RESET}"
    exit 0
fi

# ── Step runner ───────────────────────────────────────────────────────────────
STEP=0
declare -a STEP_NAMES=()
declare -a STEP_TIMES=()
declare -a STEP_STATUS=()

run_step() {
    local name="$1"; shift
    local cmd=("$@")
    STEP=$((STEP + 1))

    printf "${BOLD}[%02d]${RESET} %-45s " "$STEP" "$name"

    if $DRY_RUN; then
        echo -e "${YELLOW}(skipped — dry run)${RESET}"
        STEP_NAMES+=("$name")
        STEP_TIMES+=("—")
        STEP_STATUS+=("skip")
        return
    fi

    local t0=$SECONDS
    local log_file
    log_file=$(mktemp /tmp/mfdata_step_XXXXXX.log)

    if "${cmd[@]}" >"$log_file" 2>&1; then
        local elapsed=$((SECONDS - t0))
        printf "${GREEN}✓${RESET}  %ds\n" "$elapsed"
        STEP_NAMES+=("$name")
        STEP_TIMES+=("${elapsed}s")
        STEP_STATUS+=("ok")
    else
        local elapsed=$((SECONDS - t0))
        printf "${RED}✗  FAILED (${elapsed}s)${RESET}\n"
        echo ""
        echo -e "${RED}── Error output ──────────────────────────────────────────────${RESET}"
        tail -30 "$log_file"
        echo -e "${RED}──────────────────────────────────────────────────────────────${RESET}"
        rm -f "$log_file"
        STEP_NAMES+=("$name")
        STEP_TIMES+=("${elapsed}s")
        STEP_STATUS+=("fail")
        echo ""
        echo -e "${RED}Pipeline aborted at step ${STEP}: ${name}${RESET}"
        print_summary
        exit 1
    fi
    rm -f "$log_file"
}

# ── Summary printer ───────────────────────────────────────────────────────────
print_summary() {
    echo ""
    echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo -e "${BOLD}  Pipeline Summary${RESET}"
    echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    local i
    for i in "${!STEP_NAMES[@]}"; do
        local status="${STEP_STATUS[$i]}"
        local icon
        case "$status" in
            ok)   icon="${GREEN}✓${RESET}" ;;
            fail) icon="${RED}✗${RESET}"   ;;
            skip) icon="${YELLOW}—${RESET}" ;;
        esac
        printf "  %b  %-45s %s\n" "$icon" "${STEP_NAMES[$i]}" "${STEP_TIMES[$i]}"
    done
    echo ""
}

# ── Pipeline steps ────────────────────────────────────────────────────────────

run_step "download_nav (${FROM_DATE} → ${TO_DATE})" \
    python3 download_nav.py --from "$FROM_DATE" --to "$TO_DATE"

run_step "load_nav" \
    python3 load_nav.py

if [[ -n "$BM_FROM" && ! "$BM_FROM" > "$TO_DATE" ]]; then
    run_step "fetch_equity_benchmarks (${BM_FROM} → ${TO_DATE})" \
        python3 fetch_equity_benchmarks.py --from "$BM_FROM" --to "$TO_DATE"
else
    run_step "fetch_equity_benchmarks (full history)" \
        python3 fetch_equity_benchmarks.py
fi

run_step "fetch_nse_tri (auto-detect per index)" \
    python3 fetch_nse_tri.py --to "$TO_DATE"

run_step "build_benchmark_daily_returns (append)" \
    python3 build_benchmark_daily_returns.py --append

run_step "build_mf_daily_returns (append)" \
    python3 build_mf_daily_returns.py --append

run_step "build_excess_returns (append)" \
    python3 build_excess_returns.py --append

run_step "build_fund_performance" \
    python3 build_fund_performance.py

run_step "build_index_fund_performance" \
    python3 build_index_fund_performance.py

run_step "build_fund_ratings" \
    python3 build_fund_ratings.py

# ── Done ──────────────────────────────────────────────────────────────────────
print_summary

if ! $DRY_RUN; then
    echo -e "${GREEN}${BOLD}All steps completed successfully.${RESET}"

    # Quick DB stats
    echo ""
    echo -e "${BOLD}DB snapshot:${RESET}"
    python3 - <<'EOF'
import sqlite3
conn = sqlite3.connect("mf.db")
stats = [
    ("nav_history rows",             "SELECT COUNT(*) FROM nav_history"),
    ("mf_daily_returns rows",        "SELECT COUNT(*) FROM mf_daily_returns"),
    ("benchmark_daily_returns rows", "SELECT COUNT(*) FROM benchmark_daily_returns"),
    ("fund_performance rows",        "SELECT COUNT(*) FROM fund_performance"),
    ("index_fund_performance rows",  "SELECT COUNT(*) FROM index_fund_performance"),
    ("fund_ratings rows",            "SELECT COUNT(*) FROM fund_ratings"),
    ("Latest NAV date",              "SELECT MAX(nav_date) FROM nav_history"),
]
for label, sql in stats:
    try:
        val = conn.execute(sql).fetchone()[0]
        print(f"  {label:<35} {val:>12,}" if isinstance(val, int) else f"  {label:<35} {val:>12}")
    except Exception:
        print(f"  {label:<35} {'n/a':>12}")
conn.close()
EOF
    echo ""
fi
