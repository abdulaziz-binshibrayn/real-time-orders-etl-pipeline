import json
import logging
import os
from datetime import datetime, timezone
from io import BytesIO
from typing import Any

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error


load_dotenv(".env.local")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "orders-raw-consumer-group")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

RAW_DATA_PREFIX = os.getenv("RAW_DATA_PREFIX", "raw/orders")


def create_kafka_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def create_minio_client() -> Minio:
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False,
    )


def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logging.info("Created MinIO bucket: %s", bucket_name)


def build_object_name(event: dict[str, Any]) -> str:
    event_time = event.get("event_time")

    if event_time:
        event_datetime = datetime.fromisoformat(event_time)
    else:
        event_datetime = datetime.now(timezone.utc)

    year = event_datetime.strftime("%Y")
    month = event_datetime.strftime("%m")
    day = event_datetime.strftime("%d")
    hour = event_datetime.strftime("%H")

    event_id = event.get("event_id", "unknown-event")

    return (
        f"{RAW_DATA_PREFIX}/"
        f"year={year}/month={month}/day={day}/hour={hour}/"
        f"{event_id}.json"
    )


def upload_event_to_minio(client: Minio, event: dict[str, Any]) -> None:
    object_name = build_object_name(event)

    event_bytes = json.dumps(event, ensure_ascii=False).encode("utf-8")
    data_stream = BytesIO(event_bytes)

    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=data_stream,
        length=len(event_bytes),
        content_type="application/json",
    )

    logging.info("Uploaded raw event to MinIO: %s", object_name)


def run() -> None:
    consumer = create_kafka_consumer()
    minio_client = create_minio_client()

    ensure_bucket_exists(minio_client, MINIO_BUCKET)

    consumer.subscribe([KAFKA_TOPIC])
    logging.info("Consuming messages from Kafka topic: %s", KAFKA_TOPIC)

    try:
        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            event = json.loads(message.value().decode("utf-8"))

            upload_event_to_minio(minio_client, event)

            consumer.commit(message=message)

            logging.info(
                "Committed Kafka message | topic=%s | partition=%s | offset=%s",
                message.topic(),
                message.partition(),
                message.offset(),
            )

    except KeyboardInterrupt:
        logging.info("Consumer stopped by user.")

    except S3Error as error:
        logging.error("MinIO error: %s", error)

    finally:
        consumer.close()
        logging.info("Consumer shutdown completed.")


if __name__ == "__main__":
    run()