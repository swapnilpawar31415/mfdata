"""
MF database schema and connection helpers.

Tables:
  schemes               — core fund identity (scheme_code, name, AMC, category, ISINs)
  scheme_characteristics — time-varying fund attributes (expense ratio, exit load, etc.)
                           Populated from a separate source; NAV downloads leave this empty.
  nav_history           — daily NAV, repurchase and sale prices per scheme
  benchmark_returns     — daily closing values for benchmark indices (NIFTY TRI, BSE, CRISIL)
                          Populated by load_benchmarks.py from CSVs downloaded via
                          download_benchmarks.py (run locally).
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "mf.db"

# ── DDL ─────────────────────────────────────────────────────────────────────

_CREATE_SCHEMES = """
CREATE TABLE IF NOT EXISTS schemes (
    scheme_code           INTEGER PRIMARY KEY,
    scheme_name           TEXT    NOT NULL,
    amc_name              TEXT,
    scheme_type           TEXT,   -- 'Open Ended Schemes' | 'Close Ended Schemes' | 'Interval Fund'
    category              TEXT,   -- e.g. 'Equity Scheme - Large Cap Fund'
    benchmark             TEXT,   -- SEBI tier-1 primary benchmark (NIFTY/NSE, or BSE where no NIFTY exists)
    benchmark_2           TEXT,   -- SEBI tier-1 secondary benchmark (BSE/CRISIL alternative)
    isin_growth           TEXT,   -- ISIN Div Payout / ISIN Growth column
    isin_div_reinvestment TEXT,
    inception_date        DATE,   -- not in NAV file; populate from a separate source
    fund_manager          TEXT,   -- not in NAV file; populate from a separate source
    first_nav_date        DATE,   -- earliest NAV date seen in loaded data
    last_nav_date         DATE,   -- most recent NAV date seen in loaded data
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_CREATE_SCHEME_CHARACTERISTICS = """
CREATE TABLE IF NOT EXISTS scheme_characteristics (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    scheme_code            INTEGER NOT NULL REFERENCES schemes(scheme_code),
    effective_date         DATE    NOT NULL,
    expense_ratio_pct      REAL,   -- % p.a.; sourced separately (e.g. AMFI factsheets)
    exit_load_desc         TEXT,   -- human-readable exit load; sourced separately
    minimum_investment_inr REAL,   -- sourced separately
    recorded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (scheme_code, effective_date)
);
"""

_CREATE_NAV_HISTORY = """
CREATE TABLE IF NOT EXISTS nav_history (
    scheme_code      INTEGER NOT NULL REFERENCES schemes(scheme_code),
    nav_date         DATE    NOT NULL,
    nav              REAL,
    repurchase_price REAL,
    sale_price       REAL,
    loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (scheme_code, nav_date)
);
"""

_CREATE_BENCHMARK_RETURNS = """
CREATE TABLE IF NOT EXISTS benchmark_returns (
    index_name  TEXT NOT NULL,   -- matches schemes.benchmark or schemes.benchmark_2
    index_date  DATE NOT NULL,
    close_value REAL NOT NULL,   -- TRI / index level (as published by source)
    source      TEXT,            -- 'niftyindices', 'bseindia', 'crisil', 'manual'
    loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (index_name, index_date)
);
"""

_CREATE_BENCHMARK_DAILY_RETURNS = """
CREATE TABLE IF NOT EXISTS benchmark_daily_returns (
    index_name   TEXT NOT NULL,
    index_date   DATE NOT NULL,
    daily_return REAL NOT NULL,   -- (close / prev_close) - 1
    PRIMARY KEY (index_name, index_date)
);
"""

_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_nav_date      ON nav_history(nav_date);",
    "CREATE INDEX IF NOT EXISTS idx_nav_scheme    ON nav_history(scheme_code);",
    "CREATE INDEX IF NOT EXISTS idx_sch_amc       ON schemes(amc_name);",
    "CREATE INDEX IF NOT EXISTS idx_sch_category  ON schemes(category);",
    "CREATE INDEX IF NOT EXISTS idx_sch_isin_g    ON schemes(isin_growth);",
    "CREATE INDEX IF NOT EXISTS idx_char_scheme   ON scheme_characteristics(scheme_code);",
    "CREATE INDEX IF NOT EXISTS idx_bm_date       ON benchmark_returns(index_date);",
    "CREATE INDEX IF NOT EXISTS idx_bm_name       ON benchmark_returns(index_name);",
    "CREATE INDEX IF NOT EXISTS idx_bdr_date      ON benchmark_daily_returns(index_date);",
    "CREATE INDEX IF NOT EXISTS idx_bdr_name      ON benchmark_daily_returns(index_name);",
]


# ── Public API ───────────────────────────────────────────────────────────────

def get_connection(db_path: Path | str | None = None) -> sqlite3.Connection:
    path = Path(db_path) if db_path else DB_PATH
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(db_path: Path | str | None = None) -> None:
    """Create all tables and indexes if they don't exist."""
    conn = get_connection(db_path)
    with conn:
        conn.execute(_CREATE_SCHEMES)
        conn.execute(_CREATE_SCHEME_CHARACTERISTICS)
        conn.execute(_CREATE_NAV_HISTORY)
        conn.execute(_CREATE_BENCHMARK_RETURNS)
        conn.execute(_CREATE_BENCHMARK_DAILY_RETURNS)
        for idx in _CREATE_INDEXES:
            conn.execute(idx)
    conn.close()
    print(f"Database ready: {db_path or DB_PATH}")


if __name__ == "__main__":
    init_db()
