import os
import json
import time
import logging
import websocket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

load_dotenv()

# --------------- Configuration ---------------
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "finnhub-trades")
SYMBOLS = os.getenv("SYMBOLS", "AAPL,MSFT,GOOGL").split(",")

# --------------- Logging ---------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def create_kafka_producer(broker: str, retries: int = 10, delay: int = 5) -> KafkaProducer:
    """Create a Kafka producer with retry logic to wait for broker readiness."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info("Connected to Kafka broker at %s", broker)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka broker not available (attempt %d/%d). Retrying in %ds...",
                attempt, retries, delay,
            )
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka broker at {broker} after {retries} attempts")


def on_message(ws, message):
    """Handle incoming WebSocket messages from Finnhub."""
    data = json.loads(message)
    if data.get("type") == "trade":
        for trade in data["data"]:
            event = {
                "symbol": trade["s"],
                "price": trade["p"],
                "volume": trade["v"],
                "timestamp": trade["t"],
                "conditions": trade.get("c", []),
            }
            logger.info("Sending trade: %s @ $%.2f", event["symbol"], event["price"])
            kafka_producer.send(KAFKA_TOPIC, value=event)
        kafka_producer.flush()


def on_error(ws, error):
    logger.error("WebSocket error: %s", error)


def on_close(ws, close_status, close_msg):
    logger.info("WebSocket closed (status=%s, msg=%s)", close_status, close_msg)


def on_open(ws):
    """Subscribe to trade data for configured symbols."""
    for symbol in SYMBOLS:
        symbol = symbol.strip()
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        logger.info("Subscribed to %s", symbol)


if __name__ == "__main__":
    if not FINNHUB_API_KEY:
        raise ValueError("FINNHUB_API_KEY is not set. Add it to .env file.")

    logger.info("Starting FinnhubProducer...")
    logger.info("Symbols: %s", SYMBOLS)
    logger.info("Kafka broker: %s, topic: %s", KAFKA_BROKER, KAFKA_TOPIC)

    kafka_producer = create_kafka_producer(KAFKA_BROKER)

    ws_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    ws = websocket.WebSocketApp(
        ws_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.on_open = on_open
    ws.run_forever()
