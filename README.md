<!-- ======================= -->
<!--        HEADER           -->
<!-- ======================= -->

<h1 align="center">📦 EQUITY DEPOT</h1>
<p align="center">
  <b>Equity Fundamental Engine</b><br>
  <sub>The backbone for professional equity research and financial analysis pipelines</sub><br>
  <sub>Built with Python • SQLite • Production-Style Data Engineering</sub>
</p>

<p align="center">
  <!-- Tech Stack -->
  <img src="https://img.shields.io/badge/Python-3.x-blue?logo=python" />
  <img src="https://img.shields.io/badge/SQLite-Relational_DB-003B57?logo=sqlite" />
  <img src="https://img.shields.io/badge/SQL-Analytics_Layer-orange?logo=postgresql" /> 
  <!-- Project Quality -->
  <img src="https://img.shields.io/badge/Architecture-Normalized-green" />
  <img src="https://img.shields.io/badge/Pipeline-Modular-blueviolet" />
  <img src="https://img.shields.io/badge/Status-Active_Development-success" />
  <!-- GitHub -->
  <img src="https://img.shields.io/github/stars/vishnuvardhanaan/equity-fundamental-engine?style=social" />
  <img src="https://img.shields.io/github/last-commit/vishnuvardhanaan/equity-fundamental-engine" />
  <!-- License -->
  <img src="https://img.shields.io/badge/License-MIT-yellow" />
</p>

---

### OVERVIEW

<div align="justify">

**Equity Depot** is a production-style data engineering project that ingests, transforms raw financial APIs into a structured data, standardizes and stores fundamental equity data from the National Stock Exchange (NSE) and Yahoo Finance into a **query-optimized relational database**.

The system is designed to serve as a **robust data backbone** for scalable financial analytics, screening engines, and BI applications.

---

### PROJECT PURPOSE

This project focuses on building the **foundational data layer** required for institutional-grade equity analytics systems. Instead of jumping directly to dashboards, Equity Depot emphasizes:

- Reliable data ingestion
- Clean standardization
- Efficient storage design
- Downstream analytics readiness

Think of it as the **data warehouse layer for equity fundamentals**.

---

### BUSINESS PROBLEM

Raw financial data obtained from public APIs is typically:

- Semi-structured
- Inconsistent across companies
- Not analytics-ready
- Poorly organized for SQL-based querying

Meanwhile, analysts and BI developers require:

- Standardized schemas
- Clean financial metrics
- Query-efficient storage
- Reproducible data ingestion workflows

**Equity Depot** addresses this gap by transforming API-level financial data into a **structured relational warehouse optimized for analytics consumption**.

---

### PROJECT OBJECTIVE

To design and implement a **reproducible data pipeline** that:

- Extracts financial data for NSE-listed companies
- Standardizes and normalizes key financial attributes
- Stores structured datasets in SQLite
- Enables scalable SQL-based analytics
- Serves as a foundation layer for advanced scoring engines and BI dashboards

---

### DATA SOURCES

**Primary Sources**

- National Stock Exchange (NSE)
- Yahoo Finance API

**Data Coverage**

The pipeline ingests and structures the following datasets:

- NSE listed stocks universe
- Company static information
  - Sector
  - Industry
  - Company name
  - Business summary
- Company dynamic information
  - Price
  - Market capitalization
  - Employee count
  - OHLC data
- Financial statements metadata
  - Balance Sheet
  - Income Statement
  - Cash Flow Statement

---

### SYSTEM ARCHITECTURE

![alt text](<Equity Depot Architecture.png>)

---

### SAMPLE DATABASE VIEWS

Below are representative snapshots from the structured equity warehouse.

#### Equity Universe

<p align="center">
  <img src="screenshots/sample_2 (Stock List).png" width="85%">
</p>

<p align="center">
  <img src="screenshots/sample_5 (Stock List DB View).png" width="85%">
</p>

---

#### Company Information

<p align="center">
  <img src="screenshots/sample_3 (Stock Information).png" width="85%">
</p>

<p align="center">
  <img src="screenshots/sample_6 (Stock Information_Static Info DB View).png" width="85%">
</p>

<p align="center">
  <img src="screenshots/sample_7 (Stock Information_Dynamic Info DB View).png" width="85%">
</p>

---

#### Financial Statements

<p align="center">
  <img src="screenshots/sample_4 (Financial Statements).png" width="85%">
</p>

<p align="center">
  <img src="screenshots/sample_8 (Financial Statements_Balance Sheet DB View).png" width="85%">
</p>

<p align="center">
  <img src="screenshots/sample_9 (Financial Statements_Income Statement DB View).png" width="85%">
</p>

<p align="center">
  <img src="screenshots/sample_10 (Financial Statements_Cashflow Statement DB View).png" width="85%">
</p>

---

#### Pipeline Logging

<p align="center">
  <img src="screenshots/sample_11 (Stock Information Log DB View).png" width="85%">
</p>

<p align="center">
  <img src="screenshots/sample_12 (Financial Statements Log DB View).png" width="85%">
</p>

---

### DATABASE DESIGN

The project implements a **normalized relational schema** covering:

- Equity universe metadata
- Company profile attributes
- Market metrics
- Ownership structure
- Financial statement reference fields

**Schema Capabilities**

The database is designed to support:

- Efficient filtering by sector and industry
- Aggregation across key financial attributes
- Seamless integration with BI tools
- Downstream scoring logic (Phase 2)

---

### AUTOMATION LAYER

Python-based ingestion scripts are designed to:

- Fetch data programmatically from external APIs
- Standardize column structures across companies
- Handle missing and inconsistent values
- Persist normalized datasets into SQLite
- Allow scalable expansion of the equity universe

The ingestion workflow is built to be **reproducible, modular, and extensible**, aligning with real-world analytics engineering practices.

---

### 🧰 TECH STACK

**Core Technologies**

- Python — Data extraction and transformation
- SQLite — Relational data storage
- SQL — Analytical query layer
- Tkinter - User Interface (UI)

**Data Sources & Interfaces**

- Yahoo Finance API
- NSE data endpoints

---

### 📁 FOLDER STRUCTURE

```text
Equity-Depot/
├── data/
│   ├── nse_equity_universe_bronze.db
│   ├── nse_equity_universe_sample.db
│   ├── pipeline_runs.db
│   └── schema.sql
│
├── sandbox/
│   ├── data_required_list.xlsx
│   ├── nse_equity_universe_sample.sql
│   └── raw_equity_universe_test.dbcnb
│
├── screenshots/
│   ├── sample_1.png
│   ├── sample_2 (Stock List).png
│   ├── sample_3 (Stock Information).png
│   ├── sample_4 (Financial Statements).png
│   ├── sample_5 (Stock List DB View).png
│   ├── sample_6 (Stock Information_Static Info DB View).png
│   ├── sample_7 (Stock Information_Dynamic Info DB View).png
│   ├── sample_8 (Financial Statements_Balance Sheet DB View).png
│   ├── sample_9 (Financial Statements_Income Statement DB View).png
│   ├── sample_10 (Financial Statements_Cashflow Statement DB View).png
│   ├── sample_11 (Stock Information Log DB View).png
│   └── sample_12 (Financial Statements Log DB View).png
│
├── scripts/
│   └── run_pipeline.py
│
├── src/
│   ├── equity_pipeline/
│   │   ├── config.py
│   │   ├── db.py
│   │   ├── logging_config.py
│   │   ├── paths.py
│   │   ├── pipeline.py
│   │   ├── runner.py
│   │   ├── run_summary.py
│   │   └── ingestion/
│   │       ├── equity_info.py
│   │       ├── equity_statements.py
│   │       └── equity_universe.py
│   │
│   └── equity_pipeline.egg-info/
│       ├── dependency_links.txt
│       ├── entry_points.txt
│       ├── PKG-INFO
│       ├── SOURCES.txt
│       └── top_level.txt
│
├── README.md
├── requirement.txt
├── pyproject.toml
└── LICENSE
```

---

### SAMPLE ANALYTICAL CAPABILITIES ENABLED

Once structured, the database supports analytical queries such as:

- Top companies by market capitalization within each sector
- Ownership distribution across industries
- Sector-wise equity universe distribution
- Filtering by float shares and insider holdings
- Price-to-fundamental screening

**Downstream Use Cases**

This foundation enables a reusable financial data layer for:

- BI dashboards
- Factor modeling workflows
- Quantitative scoring engines
- Financial research and screening workflows

---

### ▶️ HOW TO RUN

#### 1. Clone the Repository

```bash
git clone https://github.com/vishnuvardhanaan/equity-fundamental-engine.git
cd equity-fundamental-engine
```

#### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

#### 3. Run the Application

```bash
python -m scripts.run_pipeline

# or

python scripts/run_pipeline.py

```

1. After opening application, select **Stock List** first
2. Then click Run
3. Monitor progress via logs and UI status
4. Then select Stock Information or Financial Statements profile to fetch data of the listed stocks

Upon successful execution, the SQLite database **nse_equity_universe_bronze.db** will be automatically generated inside the data/ directory.

---

### DESIGN PHILOSOPHY

This project intentionally separates concerns across the analytics stack:

- **Data Engineering** — Equity Depot (current project)
- **Analytics & Scoring** — Next phase
- **Visualization & BI Deployment** — Next phase

This modular architecture mirrors real-world production systems where:

- Data layers are decoupled from analytics layers
- Transformations are fully reproducible
- Storage is schema-controlled and governed
- Downstream logic builds on stable data foundations

---

### ENTITY RELATIONSHIP DIAGRAM

The database is structured using a modular relational design where each domain (profile, market metrics, ownership) is separated and linked through a common equity identifier (`symbol`).

![ER Diagram](<Equity Depot ER Diagram.png>)

This structure enables:

- Domain-level isolation
- Query optimization
- Downstream feature engineering
- Future extensibility for scoring engines

---

### WHY THIS MATTERS

Financial analytics systems are only as strong as their underlying data foundations.

In real-world finance teams:

- Raw API data is unreliable for direct analytical use
- Inconsistent schemas reduce analytical efficiency
- Lack of normalization limits BI scalability
- Poor structuring increases model fragility

**Equity Depot demonstrates how to:**

- Design structured financial data layers
- Normalize heterogeneous API outputs
- Prepare relational schemas for SQL-based analytics
- Build reusable financial datasets for downstream modeling

This project reflects how financial institutions and analytics teams architect data pipelines **before** applying scoring, modeling, or visualization layers.

---

### PROJECT IMPACT

**This project demonstrates proficiency in:**

- API-based data ingestion
- Financial data standardization
- Relational schema design
- SQL-first analytics preparation
- Modular pipeline architecture
- Analytics engineering mindset

**Role Alignment**

- Data Analyst
- BI Developer
- Financial Data Analyst
- Financial Data Scientist

---

### 👤 AUTHOR

**Vishnu Vardhanaan S**

- 💼 Data & Analytics Engineer
- 📊 Specialization: Financial Analytics • Risk Analytics • Data Pipelines • BI Systems
- 🔗 GitHub: https://github.com/vishnuvardhanaan
- 🔗 LinkedIn: https://linkedin.com/in/vishnuvardhanaan

---

### ⭐ SUPPORT THE PROJECT

If you found this project useful, consider giving it a ⭐ on GitHub — it helps increase visibility and motivates further development.

---

### 📄 LICENSE

MIT License

Copyright (c) 2025 Vishnu Vardhanaan S

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

</div>
