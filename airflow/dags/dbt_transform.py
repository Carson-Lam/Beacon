"""
dbt_transform

Weekday 7pm ET rebuild of the warehouse, once daily_refresh has landed the day's data.

- wait_for_daily_refresh: ExternalTaskSensor on the matching daily_refresh run 
- dbt_build: seeds, models and tests 
- dbt_docs_generate: refreshes the static docs page in dbt/target/

dbt runs inside the scheduler from its own virtualenv against the project mounted at /opt/airflow/dbt

"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

DBT_DIR = "/opt/airflow/dbt"
DBT = "/opt/dbt-venv/bin/dbt"
DBT_FLAGS = f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target-path target/airflow"

default_args = {
    "owner": "beacon",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_transform",
    description="Weekday 7pm ET: dbt build (models + tests) after daily_refresh, then docs",
    default_args=default_args,
    schedule="0 19 * * 1-5",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["dbt", "warehouse"],
) as dag:

    wait_for_daily_refresh = ExternalTaskSensor(
        task_id="wait_for_daily_refresh",
        external_dag_id="daily_refresh",
        external_task_id=None, 

        execution_delta=timedelta(hours=1),
        allowed_states=["success"],
        failed_states=["failed"],  # fail fast
        mode="reschedule",
        poke_interval=300,
        timeout=2 * 60 * 60,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build {DBT_FLAGS}",
        cwd=DBT_DIR,
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"{DBT} docs generate --static {DBT_FLAGS}",
        cwd=DBT_DIR,
    )

    wait_for_daily_refresh >> dbt_build >> dbt_docs_generate