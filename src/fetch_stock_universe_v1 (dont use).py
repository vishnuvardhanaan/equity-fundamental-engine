# Fetch Updated NSE Equities List

import sqlite3
from datetime import date
from io import StringIO

import pandas as pd
import requests

# -----------------------------
# CONFIG
# -----------------------------
NSE_CSV_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
DB_PATH = "data/nse_equity_universe.db"


# -----------------------------
# DOWNLOAD CSV FROM NSE
# -----------------------------
def fetch_nse_equity_csv(url: str) -> pd.DataFrame:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/csv",
        "Referer": "https://www.nseindia.com/",
    }

    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()

    return pd.read_csv(StringIO(response.text))


# -----------------------------
# PROCESS DATA
# -----------------------------
def process_equity_universe(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.upper()

    required_cols = [
        "SYMBOL",
        "NAME OF COMPANY",
        "SERIES",
        "DATE OF LISTING",
        "ISIN NUMBER",
    ]
    # Explicit column selection
    df = df.loc[:, required_cols]

    # Keep only equity series
    df = df.loc[df["SERIES"] == "EQ"]

    df = df.rename(
        columns={
            "SYMBOL": "symbol",
            "NAME OF COMPANY": "company_name",
            "SERIES": "series",
            "DATE OF LISTING": "listing_date",
            "ISIN NUMBER": "isin",
        }
    )

    # Date normalization
    df["listing_date"] = pd.to_datetime(
        df["listing_date"],
        format="%d-%b-%Y",
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")

    df["ticker"] = df["symbol"].astype(str).add(".NS")

    df["last_updated"] = date.today().isoformat()

    df = df.drop_duplicates(subset=["isin"])

    return df.reset_index(drop=True)


# -----------------------------
# DELETE REMOVED SYMBOLS FROM SQLITE DATABASE
# -----------------------------


def delete_removed_equities(df: pd.DataFrame, db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    current_symbols = set(df["symbol"].tolist())

    placeholders = ",".join("?" for _ in current_symbols)

    delete_query = f"""
        DELETE FROM raw_equity_universe
        WHERE symbol NOT IN ({placeholders})
    """

    if current_symbols:
        cur.execute(delete_query, list(current_symbols))
    else:
        # Safety: if NSE sends empty data, do NOT wipe table
        raise RuntimeError("Processed equity universe is empty. Aborting delete.")

    conn.commit()
    conn.close()


# -----------------------------
# STORE / UPSERT INTO SQLITE
# -----------------------------
def upsert_equity_universe(df: pd.DataFrame, db_path: str):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=DELETE;")
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_equity_universe (
            symbol TEXT PRIMARY KEY,
            ticker TEXT,
            isin TEXT,
            company_name TEXT,
            series TEXT,
            listing_date DATE,
            last_updated DATE
        )
    """
    )

    upsert_query = """
        INSERT INTO raw_equity_universe (symbol, ticker, isin, company_name, series, listing_date, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            ticker = excluded.ticker,
            isin = excluded.isin,
            company_name = excluded.company_name,
            series = excluded.series,
            listing_date = excluded.listing_date,
            last_updated = excluded.last_updated
    """

    cur.executemany(
        upsert_query,
        df[
            [
                "symbol",
                "ticker",
                "isin",
                "company_name",
                "series",
                "listing_date",
                "last_updated",
            ]
        ].values.tolist(),
    )

    conn.commit()
    conn.close()


# -----------------------------
# PIPELINE ENTRYPOINT
# -----------------------------
def run_equity_universe_pipeline():
    raw_df = fetch_nse_equity_csv(NSE_CSV_URL)
    processed_df = process_equity_universe(raw_df)
    delete_removed_equities(processed_df, DB_PATH)
    upsert_equity_universe(processed_df, DB_PATH)

    print(f"NSE equity universe updated: {len(processed_df)} symbols")


if __name__ == "__main__":
    run_equity_universe_pipeline()
