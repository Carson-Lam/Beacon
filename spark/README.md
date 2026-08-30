# spark

PySpark Structured Streaming consumers for Beacon.

## Local setup

Run 
```
docker compose up -d zookeeper kafka kafka-init spark-master spark-worker
```

Spark master UI: http://localhost:8080

Spark worker(s) should register automatically and show up on that page.

## Connecting to Kafka from Spark

Spark runs as separate containers on the same Docker network as Kafka (internal) listener

- `kafka:29092` for inside `spark.readStream` Kafka options
- `localhost:9092` for if you're running a Spark driver directly on the host

## Running a consumer

run 
```
docker exec -it beacon-spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-apps/consumers/equities_consumer.py
```

Output Parquet lands under `/opt/spark-data/`, bind-mounted to `./data/` on
the host.

## Structure
```
spark/
├── consumers/      
├── indicators/  
└── requirements.txt
```