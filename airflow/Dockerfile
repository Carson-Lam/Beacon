FROM apache/airflow:2.9.0

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# dbt venv
USER root
RUN python -m venv /opt/dbt-venv \ 
    && /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.5 dbt-duckdb==1.11.0
USER airflow

RUN /opt/dbt-venv/bin/python -c "import duckdb; duckdb.sql('INSTALL delta')" \
    && chmod -R g+rwX /home/airflow/.duckdb