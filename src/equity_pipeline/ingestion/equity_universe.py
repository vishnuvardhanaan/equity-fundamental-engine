import logging
import sqlite3
from datetime import datetime
from io import StringIO
from typing import Set

import pandas as pd
import requests

from equity_pipeline.paths import DB_PATH

# -----------------------------
# CONFIG
# -----------------------------

NSE_CSV_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

MIN_EXPECTED_ROWS = 1000
REQUEST_TIMEOUT = 20


# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)


# -----------------------------
# DOWNLOAD CSV FROM NSE
# -----------------------------
def fetch_nse_equity_csv(url: str) -> pd.DataFrame:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.nseindia.com/",
            "Accept": "text/csv,application/vnd.ms-excel",
        }
    )

    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    # Defensive check: NSE sometimes returns HTML with 200 OK
    if "text/html" in response.headers.get("Content-Type", "").lower():
        raise RuntimeError("NSE returned HTML instead of CSV.")

    df = pd.read_csv(StringIO(response.text))

    # Another defensive check
    if df.shape[1] <= 1:
        raise RuntimeError("CSV parsing failed; suspicious column count.")

    return df


# -----------------------------
# PROCESS DATA (RAW SNAPSHOT)
# -----------------------------
def process_equity_universe(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.upper()

    # Normalize date
    df["DATE OF LISTING"] = pd.to_datetime(
        df["DATE OF LISTING"],
        format="%d-%b-%Y",
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")

    # Normalize numeric fields explicitly
    for col in ("PAID UP VALUE", "MARKET LOT", "FACE VALUE"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Ingestion Source metadata
    df["SOURCE"] = "NSE"

    # Ingestion timestamp metadata
    df["INGESTED_TS"] = datetime.utcnow().isoformat()

    # De-duplication (business key = ISIN)
    df = df.drop_duplicates(subset=["ISIN NUMBER"])

    return df.reset_index(drop=True)


# -----------------------------
# SNAPSHOT VALIDATION
# -----------------------------
def validate_snapshot(df: pd.DataFrame) -> None:
    required_columns: Set[str] = {
        "SYMBOL",
        "NAME OF COMPANY",
        "SERIES",
        "DATE OF LISTING",
        "PAID UP VALUE",
        "MARKET LOT",
        "ISIN NUMBER",
        "FACE VALUE",
        "SOURCE",
        "INGESTED_TS",
    }

    if df.empty:
        raise RuntimeError("NSE CSV is empty. Aborting refresh.")

    if len(df) < MIN_EXPECTED_ROWS:
        raise RuntimeError(
            f"NSE CSV row count too low ({len(df)} rows). Aborting refresh."
        )

    missing = required_columns - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}")

    if df["ISIN NUMBER"].isna().any():
        raise RuntimeError("ISIN NUMBER contains NULLs.")


# -----------------------------
# SAFE SNAPSHOT REFRESH
# -----------------------------
def refresh_equity_universe(df: pd.DataFrame, db_path: str) -> None:
    validate_snapshot(df)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        try:
            cur.execute("BEGIN")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_equity_universe (
                    SYMBOL TEXT PRIMARY KEY,
                    NAME_OF_COMPANY TEXT,
                    SERIES TEXT,
                    DATE_OF_LISTING DATE,
                    PAID_UP_VALUE INTEGER,
                    MARKET_LOT INTEGER,
                    ISIN_NUMBER TEXT,
                    FACE_VALUE INTEGER,
                    SOURCE TEXT NOT NULL,
                    INGESTED_TS TEXT
                )
                """
            )

            # Atomic snapshot overwrite
            cur.execute("DELETE FROM raw_equity_universe")

            insert_query = """
                INSERT INTO raw_equity_universe (
                    SYMBOL,
                    NAME_OF_COMPANY,
                    SERIES,
                    DATE_OF_LISTING,
                    PAID_UP_VALUE,
                    MARKET_LOT,
                    ISIN_NUMBER,
                    FACE_VALUE,
                    SOURCE,
                    INGESTED_TS
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            cur.executemany(
                insert_query,
                df[
                    [
                        "SYMBOL",
                        "NAME OF COMPANY",
                        "SERIES",
                        "DATE OF LISTING",
                        "PAID UP VALUE",
                        "MARKET LOT",
                        "ISIN NUMBER",
                        "FACE VALUE",
                        "SOURCE",
                        "INGESTED_TS",
                    ]
                ].itertuples(index=False, name=None),
            )

            conn.commit()

        except Exception:
            conn.rollback()
            raise


# -----------------------------
# PIPELINE ENTRYPOINT
# -----------------------------
def run_equity_universe_pipeline() -> None:
    logging.info("Starting NSE equity universe refresh")

    raw_df = fetch_nse_equity_csv(NSE_CSV_URL)
    processed_df = process_equity_universe(raw_df)
    refresh_equity_universe(processed_df, DB_PATH)

    logging.info(
        "NSE equity universe refreshed successfully: %s symbols",
        len(processed_df),
    )


def main():
    run_equity_universe_pipeline()


if __name__ == "__main__":
    main()
