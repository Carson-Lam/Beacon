"""
Loader for config/tickers.csv,

Temporary for development

"""

import csv
from pathlib import Path

CSV_PATH = Path(__file__).resolve().with_name("tickers.csv")
COLUMNS = ("ticker", "asset_class", "alpaca_symbol", "yfinance_symbol", "stocktwits_symbol", "live")


def load_universe(asset_class=None, live=None, path=CSV_PATH):
    """Rows of tickers.csv as dicts with live parsed to bool """
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        if tuple(reader.fieldnames or ()) != COLUMNS:
            raise ValueError(f"{path}: expected columns {COLUMNS}, got {reader.fieldnames}")
        rows = list(reader)

    selected = []
    for row in rows:
        empty = [c for c in COLUMNS if not (row[c] or "").strip()]
        if empty:
            raise ValueError(f"{path}: {row['ticker'] or '<blank>'} has empty {empty}")

        flag = row["live"].strip().lower()
        if flag not in ("true", "false"):
            raise ValueError(f"{path}: {row['ticker']}: live must be true or false, got {row['live']!r}")
        row["live"] = flag == "true"

        if asset_class is not None and row["asset_class"] != asset_class:
            continue
        if live is not None and row["live"] != live:
            continue
        selected.append(row)
    return selected