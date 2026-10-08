"""
daily_refresh

Recurring weekday DAG (6pm ET) that pulls the most 
recent trading day's OHLCV per tracked equity, validates it, merges it
into existing history, recomputes indicators over the full series, and
overwrites the historical feature store.

"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

TMP_BASE = "/opt/airflow/data/_tmp/daily_refresh"
TABLE_PATH = "/opt/airflow/data/silver/equities_historical"
CONFIG_PATH = "/opt/airflow/config/tickers.csv"


CONFIG_PATH = "/opt/airflow/config/tickers.csv"

def load_tickers():
    import csv
    with open(CONFIG_PATH, newline="") as f:
        return [row["ticker"] for row in csv.DictReader(f) if row["asset_class"] == "equity"]

def fetch_daily(**context):
    import pandas as pd
    import yfinance as yf

    tickers = load_tickers()

    # context["ds"] is Airflow's *logical date* for this run 
    # for a daily DAG, this is the start of the scheduled interval
    # so 6pm yesterday, this is typically yesterday's date
    target_date = context["ds"] # testing date "2026-09-25"
    end_date = (datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    os.makedirs(f"{TMP_BASE}/raw", exist_ok=True)

    for ticker in tickers:
        df = yf.download(ticker, start=target_date, end=end_date, interval="1d", progress=False)
        df = df.dropna()

        # yfinance returns multiIndex columns for some tickers like (ticker,field)
        # flatten this to just plain field names like ("Open")
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        if df.empty:
            # yfinance returns empty object for non-trading days
            # log and skip non trading days
            print(f"[FETCH] {ticker}: 0 bar(s) for {target_date}: no file written")
            continue

        df = df.reset_index()
        df.to_parquet(f"{TMP_BASE}/raw/{ticker}.parquet", index=False)
        print(f"[FETCH] {ticker}: {len(df)} bar(s) for {target_date}")


def validate_data(**context):
    import pandas as pd

    tickers = load_tickers()
    required_cols = ["Open", "High", "Low", "Close", "Volume"]

    for ticker in tickers:
        raw_path = f"{TMP_BASE}/raw/{ticker}.parquet"

        if not os.path.exists(raw_path):
            # yfinance returns no rows for non trading days
            # log and skip non trading days
            print(f"[VALIDATE] {ticker}: no file (likely a market holiday), skipping")
            continue

        df = pd.read_parquet(raw_path)

        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"{ticker}: missing expected columns {missing}")

        nulls = df[required_cols].isnull().sum().sum()
        if nulls > 0:
            raise ValueError(f"{ticker}: {nulls} null values in OHLCV columns")

        print(f"[VALIDATE] {ticker}: {len(df)} row(s), no nulls, OK")


def compute_indicators_task(**context):
    import sys
    sys.path.append("/opt/airflow/shared/spark")
    from indicators.ta_indicators import bollinger_bands, macd, rsi, sma
    import pandas as pd
    from deltalake import DeltaTable

    tickers = load_tickers()
    os.makedirs(f"{TMP_BASE}/features", exist_ok=True)
    table = DeltaTable(TABLE_PATH)

    for ticker in tickers:
        raw_path = f"{TMP_BASE}/raw/{ticker}.parquet"
        if not os.path.exists(raw_path):
            print(f"[INDICATORS] {ticker}: no new data this run, skipping")
            continue

        new_raw = pd.read_parquet(raw_path)
        new_row = pd.DataFrame({
            "date": pd.to_datetime(new_raw["Date"]).dt.date,
            "close": new_raw["Close"].astype(float),
        })
        existing = table.to_pandas(partitions=[("symbol", "=", ticker)], columns=["date", "close"])

        # De-duplicate on date so a re-run of an existing day doesn't double-count it
        combined = pd.concat([existing, new_row], ignore_index=True)
        combined = combined.drop_duplicates(subset="date", keep="last").sort_values("date").reset_index(drop=True)

        close = combined["close"]
        bb_upper, bb_middle, bb_lower = bollinger_bands(close, 20, 2)
        macd_line, macd_signal, macd_hist = macd(close, 12, 26, 9)

        features = pd.DataFrame({
            "symbol": ticker,
            "date": combined["date"],
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

        new_rows = features[features["date"].isin(set(new_row["date"]))]
        new_rows.to_parquet(f"{TMP_BASE}/features/{ticker}.parquet", index=False)
        print(f"[INDICATORS] {ticker}: computed over {len(features)} rows, {len(new_rows)} to merge")


def write_delta_task(**context):
    import shutil
    import pandas as pd
    import pyarrow as pa
    from deltalake import DeltaTable

    tickers = load_tickers()
    paths = [f"{TMP_BASE}/features/{t}.parquet" for t in tickers]
    paths = [p for p in paths if os.path.exists(p)]

    if not paths:
        print("[WRITE] no new data this run (likely a market holiday), nothing to merge")
        shutil.rmtree(TMP_BASE, ignore_errors=True)
        return

    source = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)

    metrics = (
        DeltaTable(TABLE_PATH)
        .merge(
            source=pa.Table.from_pandas(source, preserve_index=False),
            predicate="t.symbol = s.symbol AND t.date = s.date",
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    print(f"[MERGE] inserted={metrics.get('num_target_rows_inserted')} "
          f"updated={metrics.get('num_target_rows_updated')}")

    shutil.rmtree(TMP_BASE, ignore_errors=True)


def notify_slack(**context):
    print("[SLACK] daily_refresh completed (stub, no real alert sent)")


default_args = {
    "owner": "beacon",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_refresh",
    description="Weekday 6pm ET refresh: latest daily OHLCV + recomputed indicators for tracked equities",
    default_args=default_args,
    schedule="0 18 * * 1-5",
    start_date=datetime(2026, 9, 1),
    catchup=False,  # Make sure airflow doessn't backfill missed weekdays
                    # if the DAG is unpaused. By default this is True.
    tags=["daily", "equities"],
) as dag:

    fetch_daily_task = PythonOperator(
        task_id="fetch_daily",
        python_callable=fetch_daily,
    )

    validate_data_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    compute_indicators_op = PythonOperator(
        task_id="compute_indicators",
        python_callable=compute_indicators_task,
    )

    write_delta_op = PythonOperator(
        task_id="write_delta",
        python_callable=write_delta_task,
    )

    notify_slack_op = PythonOperator(
        task_id="notify_slack",
        python_callable=notify_slack,
    )

    fetch_daily_task >> validate_data_task >> compute_indicators_op >> write_delta_op >> notify_slack_op