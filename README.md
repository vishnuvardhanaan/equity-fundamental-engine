📊 Equity Fundamental Engine

A modular, profile-driven Python data pipeline for ingesting, processing, and storing equity fundamental data with GUI + CLI execution, retry logic, logging, and SQLite persistence.

Built as a clean, extensible foundation for financial data engineering workflows.

🚀 Features

Profile-based execution

Stock List, Stock Information and Financial Statements pipeline profiles

Easily extensible for future schedules

Modular pipeline architecture

Independent ingestion steps

Clear separation of concerns

Tkinter GUI

One-click execution

Live status updates

Cancel support

Robust execution engine

Retries with exponential backoff

Graceful cancellation

Centralized logging

SQLite persistence

Structured storage for equity universe, metadata, and financial statements

Production-ready design

Thread-safe UI execution

Configurable pipeline steps

Clean project layout

🏗 Project Structure
equity-fundamental-engine/
│
├── src/
│ └── equity_pipeline/
│ ├── ingestion/
│ │ ├── equity_universe.py
│ │ ├── equity_info.py
│ │ └── equity_statements.py
│ │
│ ├── runner.py # Retry & execution logic
│ ├── pipeline.py # Tkinter UI + profile selector
│ └── **init**.py
│
├── data/
│ └── nse_equity_universe.db # SQLite database
│
├── scripts/
│ └── run_pipeline.py # Entry point
│
├── README.md
├── pyproject.toml
└── requirements.txt

⚙️ Pipeline Profiles

The pipeline supports multiple execution profiles:

Profile Description

1. Stock List - List of all available stocks currently in National Stock Exchange (NSE).
2. Stock Information - Detailed Information of all the listed stocks in National Stock Exchange (NSE).
3. Financial Statements - Statements of Balance Sheet, Income Statement and Cashflow statement of all stocks in National Stock Exchange (NSE).

Profiles are selected dynamically at runtime from the UI or CLI.

▶️ How to Run
1️⃣ Clone the Repository
git clone https://github.com/vishnuvardhanaan/equity-fundamental-engine.git
cd equity-fundamental-engine

2️⃣ Create Virtual Environment (Recommended)
python -m venv .venv
source .venv/bin/activate # Linux / Mac
.venv\Scripts\activate # Windows

3️⃣ Install Dependencies
pip install -r requirements.txt

4️⃣ Run the Application (GUI)
python scripts/run_pipeline.py

Select Stock List first

Click Run

Monitor progress via logs and UI status

Then select Stock Information or Financial Statements profile to fetch data of the listed stocks

🧪 Logging

All pipeline activity is logged with timestamps and severity levels:

Step start / success / failure

Retry attempts

Execution duration

Cancellation events

Example:

2026-01-03 22:45:24 | INFO | ▶ Starting Equity Statements (attempt 1)
2026-01-03 22:45:27 | INFO | ✓ Completed Equity Statements in 3.12s

Logs help with:

Debugging failures

Performance analysis

Auditability

🗄 Database

Database: SQLite

Location: data/equity_fundamentals.db

Designed for:

Easy inspection

Incremental expansion

Migration to PostgreSQL / DuckDB later

🧠 Design Philosophy

This project intentionally focuses on:

Clarity over cleverness

Explicit pipeline steps

Operational safety

Extensibility for future layers (silver / gold)

It is suitable as:

A learning project

A portfolio project

A base for larger financial data platforms

🔮 Future Enhancements

CLI execution with arguments (--profile weekly)

Scheduling (cron / Windows Task Scheduler)

Data validation layer

Incremental ingestion

Migration to DuckDB or PostgreSQL

Dashboard / analytics layer

📌 Tech Stack

Python 3.10+

Tkinter

SQLite

Logging

Threading

yfinance (data source)

🧑‍💻 Author

Built by Champ
Focused on clean data engineering, modular design, and production-ready pipelines.

📄 License

MIT License
