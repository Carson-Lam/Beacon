"""
lakehouse_maintenance

Nightly (2am ET) cleanup for every Delta table in the lakehouse

- optimize_tables: packs small files into larger ones. Necessary because 
  Streaming writers commit every 30-60s 

- vacuum_tables: deletes data files the table no longer references once they are older
  than 7 days. They are still queriable due to Delta Lake's "Time Travel"

Run this while the streaming writers are stopped to prevent Delta Lake transaction 
conflicts caused by concurrent access through the Docker bind mount
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_ROOT = "/opt/airflow/data"
TABLES = [
    "bronze/kafka",
    "silver/ohlcv/crypto/1min",
    "silver/ohlcv/crypto/5min",
    "silver/indicators/crypto",
    "silver/ohlcv/equities/1min",
    "silver/ohlcv/equities/5min",
    "silver/indicators/equities",
    "silver/equities_historical",
    "silver/sentiment/scored",
    "silver/sentiment/aggregates",
]
VACUUM_RETENTION_HOURS = 7 * 24


def _for_each_table(label, action):
    """Run action on every existing table and log failures"""
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError

    failed = []
    for rel in TABLES:
        path = f"{DATA_ROOT}/{rel}"
        try:
            dt = DeltaTable(path)
        except TableNotFoundError:
            print(f"[{label}] {rel}: not created yet, skipping")
            continue
        try:
            action(rel, path, dt)
        except Exception as e:
            print(f"[{label}] {rel}: FAILED: {e}")
            failed.append(rel)

    if failed:
        raise RuntimeError(f"{label} failed for: {failed}")


def optimize_tables(**context):
    from deltalake import DeltaTable

    def compact(rel, path, dt):
        before = len(dt.files())
        metrics = dt.optimize.compact()
        after = len(DeltaTable(path).files())
        print(f"[OPTIMIZE] {rel}: active files {before} -> {after} "
              f"(added={metrics.get('numFilesAdded')}, removed={metrics.get('numFilesRemoved')})")

    _for_each_table("OPTIMIZE", compact)


def vacuum_tables(**context):
    def vacuum(rel, path, dt):
        deleted = dt.vacuum(
            retention_hours=VACUUM_RETENTION_HOURS,
            dry_run=False,
            enforce_retention_duration=True,
        )
        print(f"[VACUUM] {rel}: deleted {len(deleted)} unreferenced file(s) older than 7 days")

    _for_each_table("VACUUM", vacuum)


default_args = {
    "owner": "beacon",
    "retries": 1,
}

with DAG(
    dag_id="lakehouse_maintenance",
    description="Nightly OPTIMIZE and VACUUM for all Delta tables",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["maintenance", "lakehouse"],
) as dag:

    optimize_op = PythonOperator(
        task_id="optimize_tables",
        python_callable=optimize_tables,
    )

    vacuum_op = PythonOperator(
        task_id="vacuum_tables",
        python_callable=vacuum_tables,
    )

    optimize_op >> vacuum_op