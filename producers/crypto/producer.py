import json
import os
from dotenv import load_dotenv
from kafka import KafkaProducer
from alpaca.data.live import CryptoDataStream

load_dotenv()

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SYMBOLS = ["BTC/USD", "ETH/USD"]
TOPIC = "market.crypto"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


async def on_trade(trade):
    payload = {
        "type": "trade",
        "symbol": trade.symbol,
        "price": float(trade.price),
        "size": float(trade.size),
        "timestamp": trade.timestamp.isoformat()
    }
    producer.send(TOPIC, value=payload)
    print(f"[TRADE] {trade.symbol} @ {trade.price}")


async def on_quote(quote):
    payload = {
        "type": "quote",
        "symbol": quote.symbol,
        "bid": float(quote.bid_price),
        "ask": float(quote.ask_price),
        "timestamp": quote.timestamp.isoformat()
    }
    producer.send(TOPIC, value=payload)
    print(f"[QUOTE] {quote.symbol} bid={quote.bid_price} ask={quote.ask_price}")

stream = CryptoDataStream(ALPACA_API_KEY, ALPACA_SECRET_KEY)
stream.subscribe_trades(on_trade, *SYMBOLS)
stream.subscribe_quotes(on_quote, *SYMBOLS)

print(f"Starting crypto stream for: {SYMBOLS}")
stream.run()
