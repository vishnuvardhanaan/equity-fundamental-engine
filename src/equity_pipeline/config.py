from equity_pipeline.ingestion import equity_info, equity_statements, equity_universe

PIPELINE_PROFILES = {
    "Stock List": [
        ("Equity Universe", equity_universe.main),
    ],
    "Stock Information": [
        ("Equity Info", equity_info.main),
    ],
    "Financial Statements": [
        ("Equity Statements", equity_statements.main),
    ],
}
