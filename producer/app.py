import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv


load_dotenv(".env.local")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders_raw_events")
DUMMYJSON_CARTS_URL = os.getenv("DUMMYJSON_CARTS_URL", "https://dummyjson.com/carts")

PRODUCER_INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "3"))
DATA_QUALITY_ISSUE_RATE = float(os.getenv("DATA_QUALITY_ISSUE_RATE", "0.35"))


def create_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "orders-producer",
        }
    )


def fetch_carts() -> list[dict[str, Any]]:
    response = requests.get(DUMMYJSON_CARTS_URL, timeout=10)
    response.raise_for_status()

    data = response.json()
    return data.get("carts", [])


def simulate_upstream_data_quality_issue(event: dict[str, Any]) -> dict[str, Any]:
    """
    Simulates common upstream data quality issues such as missing fields,
    invalid types, and invalid numeric values. This makes the ETL pipeline
    testable against realistic data scenarios.
    """

    if random.random() > DATA_QUALITY_ISSUE_RATE:
        return event

    issue_type = random.choice(
        [
            "missing_city",
            "missing_payment_method",
            "total_as_string",
            "missing_order_status",
            "negative_delivery_fee",
        ]
    )

    if issue_type == "missing_city":
        event["city"] = None

    elif issue_type == "missing_payment_method":
        event["payment_method"] = None

    elif issue_type == "total_as_string":
        event["total_amount"] = str(event["total_amount"])

    elif issue_type == "missing_order_status":
        event["order_status"] = None

    elif issue_type == "negative_delivery_fee":
        event["delivery_fee"] = -5

    event["data_quality_issue"] = issue_type
    return event


def build_order_event(cart: dict[str, Any]) -> dict[str, Any]:
    delivery_fee = round(random.uniform(5, 25), 2)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_EVENT",

        "source": "dummyjson_carts_api",
        "source_cart_id": cart.get("id"),

        "order_id": f"ORD-{cart.get('id')}-{uuid.uuid4().hex[:8]}",
        "user_id": cart.get("userId"),
        "items_count": cart.get("totalProducts"),
        "total_quantity": cart.get("totalQuantity"),
        "total_amount": cart.get("total"),
        "discounted_total": cart.get("discountedTotal"),
        "delivery_fee": delivery_fee,
        "city": random.choice(["Riyadh", "Jeddah", "Dammam", "Khobar", "Makkah"]),
        "payment_method": random.choice(["CARD", "CASH", "APPLE_PAY", "BANK_TRANSFER"]),
        "order_status": random.choice(["CREATED", "PAID", "DELIVERED", "CANCELLED"]),
        "products": cart.get("products", []),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "data_quality_issue": None,
    }

    return simulate_upstream_data_quality_issue(event)


def delivery_callback(error, message) -> None:
    if error:
        logging.error("Failed to deliver message: %s", error)
        return

    logging.info(
        "Message delivered | topic=%s | partition=%s | offset=%s",
        message.topic(),
        message.partition(),
        message.offset(),
    )


def run() -> None:
    producer = create_producer()
    carts = fetch_carts()

    if not carts:
        logging.warning("No carts were returned from the API.")
        return

    logging.info("Fetched %s carts from DummyJSON.", len(carts))
    logging.info("Producing events to Kafka topic: %s", KAFKA_TOPIC)

    try:
        while True:
            cart = random.choice(carts)
            event = build_order_event(cart)

            producer.produce(
                topic=KAFKA_TOPIC,
                key=event["order_id"],
                value=json.dumps(event),
                callback=delivery_callback,
            )

            producer.poll(0)

            logging.info(
                "Produced order event | order_id=%s | status=%s | issue=%s",
                event["order_id"],
                event["order_status"],
                event["data_quality_issue"],
            )

            time.sleep(PRODUCER_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logging.info("Producer stopped by user.")

    finally:
        logging.info("Flushing pending Kafka messages...")
        producer.flush()
        logging.info("Producer shutdown completed.")

if __name__ == "__main__":
    run()