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
FINAL_BASE = "/opt/airflow/data/silver/equities_historical"
CONFIG_PATH = "/opt/airflow/config/tickers.yaml"

def load_tickers():
    import yaml
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    return cfg["equities"]


def fetch_history(**context):
    import pandas as pd
    import yfinance as yf

    tickers = load_tickers()
    os.makedirs(f"{TMP_BASE}/raw", exist_ok=True)

    for ticker in tickers:
        df = yf.download(ticker, period="2y", interval="1d", progress=False)
        df = df.dropna()

        # Flatten multi index columns that yfinances uses for some ETFs
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
            "date": df["Date"],
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


def write_parquet_task(**context):
    import shutil
    import pandas as pd

    tickers = load_tickers()

    for ticker in tickers:
        df = pd.read_parquet(f"{TMP_BASE}/features/{ticker}.parquet")
        out_dir = f"{FINAL_BASE}/symbol={ticker}"
        os.makedirs(out_dir, exist_ok=True)
        df.to_parquet(f"{out_dir}/data.parquet", index=False)
        print(f"[WRITE] {ticker}: wrote {len(df)} rows to {out_dir}")

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

    write_parquet_op = PythonOperator(
        task_id="write_parquet",
        python_callable=write_parquet_task,
    )

    fetch_history_task >> compute_indicators_op >> write_parquet_op