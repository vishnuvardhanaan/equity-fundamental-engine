import json
import logging
import random
import re
import sqlite3
import time
from datetime import datetime, timezone
from typing import Dict

import yfinance as yf

# =====================================================
# CONFIG
# =====================================================

DB_PATH = "data/nse_equity_universe.db"
SOURCE = "Yahoo Finance"

FETCH_SLEEP_RANGE = (1.5, 3.5)  # seconds between symbols
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0  # exponential backoff base

LOG_LEVEL = logging.INFO

# =====================================================
# LOGGING
# =====================================================

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# =====================================================
# FIELD CONFIGURATION
# =====================================================

STATIC_FIELDS = {
    "address1",
    "address2",
    "city",
    "zip",
    "country",
    "phone",
    "fax",
    "website",
    "industry",
    "industryKey",
    "industryDisp",
    "sector",
    "sectorKey",
    "sectorDisp",
    "longBusinessSummary",
    "companyOfficers",
    "governanceEpochDate",
    "compensationAsOfEpochDate",
    "irWebsite",
    "executiveTeam",
    "maxAge",
    "currency",
    "tradeable",
    "lastFiscalYearEnd",
    "nextFiscalYearEnd",
    "mostRecentQuarter",
    "lastSplitFactor",
    "lastSplitDate",
    "quoteType",
    "financialCurrency",
    "symbol",
    "language",
    "region",
    "typeDisp",
    "quoteSourceName",
    "triggerable",
    "customPriceAlertConfidence",
    "exchange",
    "messageBoardId",
    "exchangeTimezoneName",
    "exchangeTimezoneShortName",
    "gmtOffSetMilliseconds",
    "market",
    "esgPopulated",
    "shortName",
    "longName",
    "hasPrePostMarketData",
    "firstTradeDateMilliseconds",
    "fullExchangeName",
    "earningsTimestamp",
    "earningsTimestampStart",
    "earningsTimestampEnd",
    "earningsCallTimestampStart",
    "earningsCallTimestampEnd",
    "isEarningsDateEstimate",
    "sourceInterval",
    "exchangeDataDelayedBy",
    "cryptoTradeable",
}

# =====================================================
# DATABASE HELPERS
# =====================================================


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def create_base_tables(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_equity_staticinfo (
            symbol TEXT PRIMARY KEY,
            source TEXT,
            ingested_at TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_equity_dynamicinfo (
            symbol TEXT,
            as_of_date TEXT,
            source TEXT,
            ingested_at TEXT,
            PRIMARY KEY (symbol, as_of_date)
        )
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dynamic_symbol_date
        ON raw_equity_dynamicinfo (symbol, as_of_date)
        """
    )


def ensure_columns(conn: sqlite3.Connection, table: str, columns):
    existing = {row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')}

    for col in columns:
        if col not in existing:
            conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT')
            logger.debug("Added column %s to %s", col, table)


# =====================================================
# NORMALIZATION
# =====================================================


def normalize_value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def normalize_column_name(col: str) -> str:
    col = re.sub(r"[^a-zA-Z0-9_]", "_", col)
    if col and col[0].isdigit():
        col = f"f_{col}"
    return col.lower()


def split_static_dynamic(info: dict):
    static, dynamic = {}, {}

    for raw_key, raw_value in info.items():
        safe_key = normalize_column_name(raw_key)
        safe_value = normalize_value(raw_value)

        if raw_key in STATIC_FIELDS:
            static[safe_key] = safe_value
        else:
            dynamic[safe_key] = safe_value

    return static, dynamic


# =====================================================
# UPSERTS
# =====================================================


def upsert_static(conn: sqlite3.Connection, row: Dict):
    cols = list(row.keys())
    placeholders = ",".join("?" for _ in cols)
    col_sql = ",".join(f'"{c}"' for c in cols)
    updates = ",".join(f'"{c}"=excluded."{c}"' for c in cols if c != "symbol")

    sql = f"""
        INSERT INTO raw_equity_staticinfo ({col_sql})
        VALUES ({placeholders})
        ON CONFLICT(symbol) DO UPDATE SET {updates}
    """
    conn.execute(sql, list(row.values()))


def upsert_dynamic(conn: sqlite3.Connection, row: Dict):
    cols = list(row.keys())
    placeholders = ",".join("?" for _ in cols)
    col_sql = ",".join(f'"{c}"' for c in cols)
    updates = ",".join(
        f'"{c}"=excluded."{c}"' for c in cols if c not in ("symbol", "as_of_date")
    )

    sql = f"""
        INSERT INTO raw_equity_dynamicinfo ({col_sql})
        VALUES ({placeholders})
        ON CONFLICT(symbol, as_of_date)
        DO UPDATE SET {updates}
    """
    conn.execute(sql, list(row.values()))


# =====================================================
# FETCH LOGIC
# =====================================================


def to_yahoo_symbol(symbol: str) -> str:
    return f"{symbol}.NS"


def fetch_symbols(conn: sqlite3.Connection):
    return [r[0] for r in conn.execute("SELECT symbol FROM raw_equity_universe_backup")]


def fetch_info_with_retries(symbol: str) -> dict | None:
    yahoo_symbol = to_yahoo_symbol(symbol)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ticker = yf.Ticker(yahoo_symbol)
            info = ticker.get_info()

            if not isinstance(info, dict) or len(info) < 10:
                raise ValueError("Empty or invalid payload")

            return info

        except Exception as e:
            logger.warning(
                "Fetch failed for %s (attempt %d/%d): %s",
                symbol,
                attempt,
                MAX_RETRIES,
                e,
            )

            if attempt < MAX_RETRIES:
                sleep_time = RETRY_BACKOFF**attempt
                time.sleep(sleep_time)

    logger.error("Giving up on %s after %d retries", symbol, MAX_RETRIES)
    return None


# =====================================================
# INGESTION
# =====================================================


def ingest_info():
    conn = get_connection()
    create_base_tables(conn)

    symbols = fetch_symbols(conn)

    as_of_date = datetime.now(timezone.utc).date().isoformat()
    ingested_at = datetime.now(timezone.utc).isoformat()

    run_started = time.time()
    success, failed = 0, 0

    for idx, symbol in enumerate(symbols, start=1):
        logger.info("Processing %s (%d/%d)", symbol, idx, len(symbols))

        info = fetch_info_with_retries(symbol)
        if not info:
            failed += 1
            continue

        static, dynamic = split_static_dynamic(info)

        static_row = {
            "symbol": symbol,
            "source": SOURCE,
            "ingested_at": ingested_at,
            **static,
        }

        dynamic_row = {
            "symbol": symbol,
            "as_of_date": as_of_date,
            "source": SOURCE,
            "ingested_at": ingested_at,
            **dynamic,
        }

        try:
            ensure_columns(conn, "raw_equity_staticinfo", static_row.keys())
            ensure_columns(conn, "raw_equity_dynamicinfo", dynamic_row.keys())

            upsert_static(conn, static_row)
            upsert_dynamic(conn, dynamic_row)

            conn.commit()
            success += 1

        except Exception as e:
            logger.exception("DB error processing %s: %s", symbol, e)
            failed += 1

        sleep_time = random.uniform(*FETCH_SLEEP_RANGE)
        time.sleep(sleep_time)

    duration = round(time.time() - run_started, 2)
    logger.info(
        "Ingestion complete | Success: %d | Failed: %d | Duration=%ss",
        success,
        failed,
        duration,
    )

    conn.close()


# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    ingest_info()
