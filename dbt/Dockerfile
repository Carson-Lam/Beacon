FROM python:3.12-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV HOME=/home/dbt
RUN mkdir -p $HOME \
    && python -c "import duckdb; duckdb.sql('INSTALL delta')" \
    # Run as lakehouse's shared UID (185:0)
    && chown -R 185:0 $HOME 

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
    
WORKDIR /usr/app/dbt
USER 185:0