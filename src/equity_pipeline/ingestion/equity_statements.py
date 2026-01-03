import logging
import random
import sqlite3
import time
from datetime import datetime, timezone
from typing import Callable, Iterable

import pandas as pd
import yfinance as yf

from equity_pipeline.paths import DB_PATH

# =====================================================
# CONFIG
# =====================================================

SOURCE = "Yahoo Finance"
SLEEP_RANGE = (1.5, 4.5)

STATEMENTS = [
    ("raw_stock_balancesheet", lambda t: t.get_balance_sheet()),
    ("raw_stock_incomestmt", lambda t: t.get_income_stmt()),
    ("raw_stock_cashflowstmt", lambda t: t.get_cashflow()),
]


# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# =====================================================
# DATABASE
# =====================================================
def bootstrap_database(conn: sqlite3.Connection) -> None:
    fact_ddl = """
    CREATE TABLE IF NOT EXISTS {table} (
        symbol TEXT NOT NULL,
        metric_name TEXT NOT NULL,
        fiscal_year INTEGER NOT NULL,
        fiscal_date TEXT NOT NULL,
        value REAL NOT NULL,
        source TEXT NOT NULL,
        ingested_ts TEXT NOT NULL,
        last_observed_ts TEXT NOT NULL,
        PRIMARY KEY (symbol, metric_name, fiscal_year)
    );
    """
    index_ddl = """
    CREATE INDEX IF NOT EXISTS idx_{table}_symbol_year
    ON {table}(symbol, fiscal_year);
    """

    audit_ddl = """
    CREATE TABLE IF NOT EXISTS raw_statements_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        table_name TEXT NOT NULL,
        ingested_ts TEXT NOT NULL,
        row_count INTEGER NOT NULL,
        status TEXT NOT NULL
    );
    """

    for table, _ in STATEMENTS:
        conn.execute(fact_ddl.format(table=table))
        conn.execute(index_ddl.format(table=table))

    conn.execute(audit_ddl)
    conn.commit()


def fetch_symbols(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT SYMBOL FROM raw_equity_universe_backup").fetchall()
    return [r[0].strip().upper() for r in rows]


# =====================================================
# UTILS
# =====================================================
def to_yahoo_symbol(symbol: str) -> str:
    return f"{symbol}.NS"


def fiscal_year_from_date(dt: pd.Timestamp) -> int:
    # Indian FY ends in March
    return dt.year - 1 if dt.month <= 3 else dt.year


def safe_fetch(
    fetcher: Callable[[yf.Ticker], pd.DataFrame],
    ticker: yf.Ticker,
) -> pd.DataFrame:
    try:
        df = fetcher(ticker)
        return df if df is not None else pd.DataFrame()
    except Exception as exc:
        logging.warning(
            "Fetch failed | %s | %s",
            ticker.ticker,
            exc,
        )
        return pd.DataFrame()


# =====================================================
# FETCH + TRANSFORM
# =====================================================
def fetch_statement_long(
    symbol: str,
    ticker: yf.Ticker,
    fetcher: Callable[[yf.Ticker], pd.DataFrame],
    ingested_ts: str,
) -> pd.DataFrame:
    df = safe_fetch(fetcher, ticker)

    if df.empty:
        return pd.DataFrame()

    # Defensive column parsing
    df.columns = pd.to_datetime(df.columns, errors="coerce")
    df = df.loc[:, df.columns.notna()]

    long_df = (
        df.reset_index()
        .melt(
            id_vars="index",
            var_name="fiscal_date",
            value_name="value",
        )
        .rename(columns={"index": "metric_name"})
    )

    # Normalize fiscal_date defensively
    long_df["fiscal_date"] = pd.to_datetime(long_df["fiscal_date"], errors="coerce")
    # Drop rows where date could not be parsed
    long_df = long_df.dropna(subset=["fiscal_date"])
    long_df["fiscal_year"] = long_df["fiscal_date"].apply(fiscal_year_from_date)
    # Store as ISO date string (SQLite-friendly, explicit)
    long_df["fiscal_date"] = long_df["fiscal_date"].dt.date

    long_df["symbol"] = symbol
    long_df["source"] = SOURCE
    long_df["ingested_ts"] = ingested_ts
    long_df["last_observed_ts"] = ingested_ts

    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    long_df = long_df.dropna(subset=["value"])

    return long_df[
        [
            "symbol",
            "metric_name",
            "fiscal_year",
            "fiscal_date",
            "value",
            "source",
            "ingested_ts",
            "last_observed_ts",
        ]
    ]


# =====================================================
# LOAD
# =====================================================
def insert_statement(
    conn: sqlite3.Connection,
    table: str,
    df: pd.DataFrame,
) -> None:
    if df.empty:
        return

    sql = f"""
        INSERT INTO {table} (
            symbol,
            metric_name,
            fiscal_year,
            fiscal_date,
            value,
            source,
            ingested_ts,
            last_observed_ts
        )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(symbol, metric_name, fiscal_year)
    DO UPDATE SET
        value = CASE
            WHEN excluded.value IS NOT NULL
                 AND excluded.value != 0
                 AND excluded.value != {table}.value
            THEN excluded.value
            ELSE {table}.value
        END,

        fiscal_date = COALESCE(excluded.fiscal_date, {table}.fiscal_date),

        ingested_ts = CASE
            WHEN excluded.value IS NOT NULL
                 AND excluded.value != 0
                 AND excluded.value != {table}.value
            THEN excluded.ingested_ts
            ELSE {table}.ingested_ts
        END,

        last_observed_ts = excluded.last_observed_ts;
    """

    conn.executemany(sql, df.itertuples(index=False, name=None))


def insert_audit_log(
    conn: sqlite3.Connection,
    symbol: str,
    table: str,
    ingested_ts: str,
    row_count: int,
    status: str,
) -> None:
    conn.execute(
        """
        INSERT INTO raw_statements_log (
            symbol,
            table_name,
            ingested_ts,
            row_count,
            status
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (symbol, table, ingested_ts, row_count, status),
    )


# =====================================================
# ORCHESTRATOR
# =====================================================
def ingest_fundamentals(
    symbols: Iterable[str],
    db_path: str = DB_PATH,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        bootstrap_database(conn)

        for symbol in symbols:
            logging.info("Ingesting fundamentals for %s", symbol)

            ticker = yf.Ticker(to_yahoo_symbol(symbol))
            ingested_ts = datetime.now(tz=timezone.utc).isoformat()

            with conn:  # atomic per symbol
                for table, fetcher in STATEMENTS:
                    try:
                        df = fetch_statement_long(
                            symbol,
                            ticker,
                            fetcher,
                            ingested_ts,
                        )

                        insert_statement(conn, table, df)

                        insert_audit_log(
                            conn,
                            symbol,
                            table,
                            ingested_ts,
                            len(df),
                            "SUCCESS",
                        )

                        if len(df) < 5:
                            logging.warning(
                                "%s | %s returned very few rows (%d)",
                                symbol,
                                table,
                                len(df),
                            )

                        logging.info(
                            "%s | %s → %d rows",
                            symbol,
                            table,
                            len(df),
                        )

                    except Exception as exc:
                        insert_audit_log(
                            conn,
                            symbol,
                            table,
                            ingested_ts,
                            0,
                            "FAILED",
                        )
                        logging.error(
                            "ERROR ingesting %s | %s → %s",
                            symbol,
                            table,
                            exc,
                        )

            time.sleep(random.uniform(*SLEEP_RANGE))


# =====================================================
# ENTRY POINT
# =====================================================
def main():
    with sqlite3.connect(DB_PATH) as conn:
        symbols = fetch_symbols(conn)

    ingest_fundamentals(symbols)


if __name__ == "__main__":
    main()
    main()
