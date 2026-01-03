import json
import logging
import random
import re
import sqlite3
import time
from datetime import datetime, timezone
from typing import Dict, Tuple

import yfinance as yf

from equity_pipeline.paths import DB_PATH

# =====================================================
# CONFIG
# =====================================================

SOURCE = "Yahoo Finance"

FETCH_SLEEP_RANGE = (1.5, 3.5)
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0

COMMIT_BATCH_SIZE = 20
MAX_DYNAMIC_COLUMNS = 300

LOG_LEVEL = logging.INFO


# =====================================================
# LOGGING
# =====================================================

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
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
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS raw_equity_staticinfo (
            symbol TEXT PRIMARY KEY,
            source TEXT,
            ingested_at TEXT
        );

        CREATE TABLE IF NOT EXISTS raw_equity_dynamicinfo (
            symbol TEXT,
            as_of_date TEXT,
            source TEXT,
            ingested_at TEXT,
            extra_payload TEXT,
            PRIMARY KEY (symbol, as_of_date)
        );

        CREATE INDEX IF NOT EXISTS idx_dynamic_symbol_date
        ON raw_equity_dynamicinfo (symbol, as_of_date);

        CREATE TABLE IF NOT EXISTS raw_info_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            table_name TEXT,
            ingested_at TEXT,
            column_count INTEGER,
            status TEXT
        );
        """
    )


def ensure_columns(conn: sqlite3.Connection, table: str, columns):
    existing = {row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')}
    for col in columns:
        if col not in existing:
            conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT')


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


def split_static_dynamic(info: dict) -> Tuple[Dict, Dict, Dict]:
    static, dynamic, extra = {}, {}, {}

    for raw_key, raw_value in info.items():
        safe_key = normalize_column_name(raw_key)
        safe_value = normalize_value(raw_value)

        if raw_key in STATIC_FIELDS:
            static[safe_key] = safe_value
        else:
            dynamic[safe_key] = safe_value

    if len(dynamic) > MAX_DYNAMIC_COLUMNS:
        extra = dynamic
        dynamic = {}

    return static, dynamic, extra


# =====================================================
# UPSERT
# =====================================================


def upsert(conn: sqlite3.Connection, table: str, row: Dict, pk_fields: Tuple[str]):
    cols = list(row.keys())
    placeholders = ",".join("?" for _ in cols)
    col_sql = ",".join(f'"{c}"' for c in cols)
    updates = ",".join(f'"{c}"=excluded."{c}"' for c in cols if c not in pk_fields)

    sql = f"""
        INSERT INTO {table} ({col_sql})
        VALUES ({placeholders})
        ON CONFLICT({",".join(pk_fields)}) DO UPDATE SET {updates}
    """
    conn.execute(sql, list(row.values()))


# =====================================================
# FETCH
# =====================================================


def to_yahoo_symbol(symbol: str) -> str:
    return f"{symbol}.NS"


def fetch_symbols(conn: sqlite3.Connection):
    return [r[0] for r in conn.execute("SELECT symbol FROM raw_equity_universe_backup")]


def fetch_info_with_retries(symbol: str) -> dict | None:
    yahoo_symbol = to_yahoo_symbol(symbol)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            info = yf.Ticker(yahoo_symbol).info
            if not info or "symbol" not in info:
                raise ValueError("Invalid Yahoo payload")
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
                time.sleep(RETRY_BACKOFF**attempt)
    return None


# =====================================================
# INGESTION
# =====================================================


def log_ingestion(conn, symbol, table_name, column_count, status):
    conn.execute(
        """
        INSERT INTO raw_info_log
        (symbol, table_name, ingested_at, column_count, status)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            symbol,
            table_name,
            datetime.now(timezone.utc).isoformat(),
            column_count,
            status,
        ),
    )


def ingest_info():
    conn = get_connection()
    create_base_tables(conn)

    symbols = fetch_symbols(conn)
    as_of_date = datetime.now(timezone.utc).date().isoformat()
    ingested_at = datetime.now(timezone.utc).isoformat()

    run_started = time.time()
    success = failed = 0

    try:
        for idx, symbol in enumerate(symbols, start=1):
            logger.info("Processing %s (%d/%d)", symbol, idx, len(symbols))

            info = fetch_info_with_retries(symbol)
            if not info:
                failed += 1
                log_ingestion(conn, symbol, "ALL", 0, "FAILED")
                continue

            static, dynamic, extra = split_static_dynamic(info)

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
                "extra_payload": json.dumps(extra) if extra else None,
            }

            ensure_columns(conn, "raw_equity_staticinfo", static_row.keys())
            ensure_columns(
                conn,
                "raw_equity_dynamicinfo",
                [c for c in dynamic_row.keys() if c != "extra_payload"],
            )

            upsert(conn, "raw_equity_staticinfo", static_row, ("symbol",))
            log_ingestion(
                conn,
                symbol,
                "raw_equity_staticinfo",
                len(static_row),
                "SUCCESS",
            )

            upsert(
                conn,
                "raw_equity_dynamicinfo",
                dynamic_row,
                ("symbol", "as_of_date"),
            )
            log_ingestion(
                conn,
                symbol,
                "raw_equity_dynamicinfo",
                len(dynamic_row),
                "SUCCESS",
            )

            success += 1

            if idx % COMMIT_BATCH_SIZE == 0:
                conn.commit()

            time.sleep(random.uniform(*FETCH_SLEEP_RANGE))

        conn.commit()

    except KeyboardInterrupt:
        logger.warning("Interrupted, committing progress")
        conn.commit()

    finally:
        conn.close()
        duration = round(time.time() - run_started, 2)
        logger.info(
            "Ingestion finished | Success=%d | Failed=%d | Duration=%ss",
            success,
            failed,
            duration,
        )


# =====================================================
# ENTRY POINT
# =====================================================


def main():
    ingest_info()


if __name__ == "__main__":
    main()
    main()
