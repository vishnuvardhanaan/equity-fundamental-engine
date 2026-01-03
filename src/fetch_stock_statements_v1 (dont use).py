import sqlite3
from datetime import datetime

import pandas as pd
import yfinance as yf

DB_PATH = "data/nse_equity_universe.db"
# STOCK_LIST = ["RELIANCE.NS", "HDFCBANK.NS", "TCS.NS", "BHARTIARTL.NS", "ITC.NS"]
STOCK_LIST = ["ONGC", "ICICIBANK", "INFY", "CIPLA", "LT"]

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# --------------------------------------------------
# DB CONNECTION
# --------------------------------------------------
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# --------------------------------------------------
# SCHEMA GUARANTEE (SELF-HEALING)
# --------------------------------------------------
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS raw_stock_balancesheet (
    stock_symbol TEXT NOT NULL,
    metric TEXT NOT NULL,
    fiscal_year INTEGER NOT NULL,
    value REAL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stock_symbol, metric, fiscal_year)
);
"""
)

# --------------------------------------------------
# FETCH SPECIFIED STOCKS (CORRECT SQL)
# --------------------------------------------------
placeholders = ",".join(["?"] * len(STOCK_LIST))

stocks = pd.read_sql(
    f"""
    SELECT symbol
    FROM raw_equity_universe
    WHERE symbol IN ({placeholders})
    """,
    conn,
    params=STOCK_LIST,
)["symbol"].tolist()

# --------------------------------------------------
# INSERT (NO OVERWRITE)
# --------------------------------------------------
insert_sql = """
INSERT OR IGNORE INTO raw_stock_balancesheet
(stock_symbol, metric, fiscal_year, value, last_updated)
VALUES (?, ?, ?, ?, ?)
"""

# --------------------------------------------------
# PROCESS
# --------------------------------------------------
for stock in stocks:
    try:
        print(f"Fetching balance sheet for {stock}")

        bs = yf.Ticker(stock + ".NS").balance_sheet

        if bs.empty:
            print(f"⚠️ No balance sheet for {stock}")
            continue

        bs.columns = [
            col.year if hasattr(col, "year") else int(col) for col in bs.columns
        ]

        for metric in bs.index:
            for year in bs.columns:
                value = bs.loc[metric, year]

                if pd.isna(value):
                    continue

                cursor.execute(
                    insert_sql,
                    (stock, metric, int(year), float(value), datetime.utcnow()),
                )

        print(f"✅ Stored balance sheet for {stock}")

    except Exception as e:
        print(f"❌ Error processing {stock}: {e}")

conn.commit()
conn.close()
conn.close()
