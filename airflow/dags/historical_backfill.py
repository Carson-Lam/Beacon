"""
historical_backfill

One-time DAG that pulls 2 years of daily OHLCV per tracked equity via
yfinance, computes the sameindicators the streaming consumer computes
live, and writes partitioned Parquet to silver/equities_historical/.

"""

import os
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TMP_BASE = "/opt/airflow/data/_tmp/historical"
TABLE_PATH = "/opt/airflow/data/silver/equities_historical"
CONFIG_PATH = "/opt/airflow/config/tickers.csv"

def load_tickers():
    import csv
    with open(CONFIG_PATH, newline="") as f:
        return [row["ticker"] for row in csv.DictReader(f) if row["asset_class"] == "equity"]


def fetch_history(**context):
    import pandas as pd
    import yfinance as yf

    tickers = load_tickers()
    os.makedirs(f"{TMP_BASE}/raw", exist_ok=True)

    for ticker in tickers:
        df = yf.download(ticker, period="2y", interval="1d", progress=False)
        df = df.dropna()

        # Flatten multi index columns that yfinance uses for some ETFs
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.reset_index()
        df.to_parquet(f"{TMP_BASE}/raw/{ticker}.parquet", index=False)
        print(f"[FETCH] {ticker}: {len(df)} daily bars")
        time.sleep(1)  # throttle for rate limits


def compute_indicators_task(**context):
    import sys
    sys.path.append("/opt/airflow/shared/spark")
    from indicators.ta_indicators import bollinger_bands, macd, rsi, sma
    import pandas as pd

    tickers = load_tickers()
    os.makedirs(f"{TMP_BASE}/features", exist_ok=True)

    for ticker in tickers:
        df = pd.read_parquet(f"{TMP_BASE}/raw/{ticker}.parquet")
        close = df["Close"].astype(float)

        bb_upper, bb_middle, bb_lower = bollinger_bands(close, 20, 2)
        macd_line, macd_signal, macd_hist = macd(close, 12, 26, 9)

        features = pd.DataFrame({
            "symbol": ticker,
            # A trading day, not a timestamp. It is half of the (symbol, date) key that
            # daily_refresh MERGEs on, so both DAGs must store it the same way.
            "date": pd.to_datetime(df["Date"]).dt.date,
            "close": close,
            "sma_20": sma(close, 20),
            "sma_50": sma(close, 50),
            "rsi_14": rsi(close, 14),
            "bb_upper": bb_upper,
            "bb_middle": bb_middle,
            "bb_lower": bb_lower,
            "macd": macd_line,
            "macd_signal": macd_signal,
            "macd_hist": macd_hist,
        })
        features.to_parquet(f"{TMP_BASE}/features/{ticker}.parquet", index=False)
        print(f"[INDICATORS] {ticker}: computed {len(features)} rows")


def write_delta_task(**context):
    import shutil
    import pandas as pd
    import pyarrow as pa
    from deltalake import write_deltalake

    tickers = load_tickers()
    df = pd.concat(
        [pd.read_parquet(f"{TMP_BASE}/features/{t}.parquet") for t in tickers],
        ignore_index=True,
    )

    table = pa.Table.from_pandas(df, preserve_index=False)
    write_deltalake(TABLE_PATH, table, mode="overwrite", partition_by=["symbol"])
    for symbol, n in df.groupby("symbol").size().items():
        print(f"[WRITE] {symbol}: {n} rows")
    print(f"[WRITE] {len(df)} total rows to {TABLE_PATH}")

    shutil.rmtree(TMP_BASE, ignore_errors=True)


default_args = {
    "owner": "beacon",
    "retries": 1,
}

with DAG(
    dag_id="historical_backfill",
    description="One-time 2yr daily OHLCV + indicator backfill for tracked equities",
    default_args=default_args,
    schedule="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["backfill", "equities"],
) as dag:

    fetch_history_task = PythonOperator(
        task_id="fetch_history",
        python_callable=fetch_history,
    )

    compute_indicators_op = PythonOperator(
        task_id="compute_indicators",
        python_callable=compute_indicators_task,
    )

    write_delta_op = PythonOperator(
        task_id="write_delta",
        python_callable=write_delta_task,
    )

    fetch_history_task >> compute_indicators_op >> write_delta_op