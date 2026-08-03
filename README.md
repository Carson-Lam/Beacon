# Beacon 
### Real-Time Financial Market Intelligence Platform

![Python](https://img.shields.io/badge/Python-3.13-blue)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6.0-black)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![CI](https://github.com/Carson_Lam/beacon/actions/workflows/ci.yml/badge.svg)

Beacon is a real-time financial market intelligence platform that ingests live market data, processes it through a streaming pipeline, and surfaces actionable research insights via a dashboard and AI agent interface.


## Architecture

> Phase 1 diagram — coming soon

**Current stack:**
- **Ingestion:** Alpaca Markets WebSocket (equities + crypto) → Apache Kafka
- **Topics:** `market.equities`, `market.crypto`, `sentiment.raw`
- **Processing:** PySpark + dbt *(Phase 2)*
- **Storage:** Delta Lake + DuckDB + Snowflake *(Phase 2)*
- **ML:** FinBERT sentiment, Isolation Forest anomaly detection *(Phase 3)*
- **Serving:** FastAPI + Streamlit + MCP server *(Phase 4)*

## Running Locally

**Prerequisites:** Docker Desktop, Python 3.13

**1. Clone the repo**
```bash
git clone https://github.com/Carson_Lam/beacon.git
cd beacon
```

**2. Set up environment variables**
```bash
cp .env.example .env
# Fill in your Alpaca API keys
```

**3. Start Kafka infrastructure**
```bash
docker compose up -d
```

**4. Run a producer**
```bash
# Crypto (24/7)
pip install -r producers/crypto/requirements.txt
python producers/crypto/producer.py

# Equities (market hours only, 9:30am–4pm ET)
pip install -r producers/equities/requirements.txt
python producers/equities/producer.py
```

## Project Structure
```
beacon/
├── producers/
│ ├── equities/ # Alpaca equities WebSocket producer
│ └── crypto/ # Alpaca crypto WebSocket producer
├── consumers/ # Kafka consumers (Phase 2)
├── docs/
│ └── adr/ # Architectural Decision Records
├── docker-compose.yml
└── .github/
└── workflows/
└── ci.yml
```

## Architectural Decision Records

- [ADR-001: Kafka as the Streaming Backbone](docs/adr/ADR-001-kafka-over-polling.md)