import os
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "orders_cleaned")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "data-lake")
RAW_DATA_PREFIX = os.getenv("RAW_DATA_PREFIX", "raw/orders")
PROCESSED_DATA_PREFIX = os.getenv("PROCESSED_DATA_PREFIX", "processed/orders")
REJECTED_DATA_PREFIX = os.getenv("REJECTED_DATA_PREFIX", "rejected/orders")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("OrdersETLJob")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ROOT_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_ROOT_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

def is_blank(column_name: str):
    return F.col(column_name).isNull() | (F.trim(F.col(column_name)) == "")

def read_raw_orders(spark: SparkSession) -> DataFrame:
    raw_path = f"s3a://{MINIO_BUCKET}/{RAW_DATA_PREFIX}/"

    logging.info("Reading raw orders from: %s", raw_path)

    return (
        spark.read
        .option("recursiveFileLookup", "true")
        .json(raw_path)
    )
def transform_orders(raw_df: DataFrame):
    orders_df = (
        raw_df
        .withColumn("total_amount_clean", F.col("total_amount").cast("double"))
        .withColumn("delivery_fee_clean", F.col("delivery_fee").cast("double"))
        .withColumn("event_time_ts", F.to_timestamp("event_time"))
        .withColumn("event_date", F.to_date(F.substring("event_time",1,10)))
        .withColumn("processed_at", F.current_timestamp())
    )

    rejection_reason = F.concat_ws(
        ",",
        F.when(is_blank("event_id"), F.lit("missing_event_id")),
        F.when(is_blank("order_id"), F.lit("missing_order_id")),
        F.when(is_blank("event_time"), F.lit("missing_event_time")),
        F.when(is_blank("city"), F.lit("missing_city")),
        F.when(is_blank("order_status"), F.lit("missing_order_status")),
        F.when(is_blank("payment_method"), F.lit("missing_payment_method")),
        F.when(F.col("total_amount").isNull(), F.lit("missing_total_amount")),
        F.when(
            F.col("total_amount").isNotNull() & F.col("total_amount_clean").isNull(),
            F.lit("invalid_total_amount"),
        ),
        F.when(F.col("delivery_fee").isNull(), F.lit("missing_delivery_fee")),
        F.when(F.col("delivery_fee_clean") < 0, F.lit("negative_delivery_fee")),
    )

    orders_df = orders_df.withColumn("rejection_reason" , rejection_reason)

    clean_orders_df = (
        orders_df
        .filter(F.col("rejection_reason") == "")
        .select(
            "event_id",
            "order_id",
            "event_time",
            "event_time_ts",
            "event_date",
            "event_type",
            "user_id",
            "source_cart_id",
            "city",
            "order_status",
            "payment_method",
            F.col("total_amount_clean").alias("total_amount"),
            "discounted_total",
            F.col("delivery_fee_clean").alias("delivery_fee"),
            "items_count",
            "total_quantity",
            "source",
            "processed_at",
        )

    )

    rejected_orders_df = (
        orders_df
        .filter(F.col("rejection_reason") != "")
        .drop("total_amount_clean", "delivery_fee_clean")
    )

    return clean_orders_df, rejected_orders_df


def write_outputs(clean_df: DataFrame, rejected_df: DataFrame) -> None:
    processed_path =  f"s3a://{MINIO_BUCKET}/{PROCESSED_DATA_PREFIX}/"
    rejected_path = f"s3a://{MINIO_BUCKET}/{REJECTED_DATA_PREFIX}/"

    clean_count = clean_df.count()
    rejected_count = rejected_df.count()

    logging.info("Clean orders: %s", clean_count)
    logging.info("Rejected orders: %s", rejected_count)

    logging.info("Writing clean orders to: %s", processed_path)
    (
        clean_df.coalesce(1).write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(processed_path)
    )

    logging.info("Writing rejected orders to: %s", rejected_path)
    (
        rejected_df.coalesce(1).write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(rejected_path)
    )

def write_to_postgres(clean_df: DataFrame) -> None:
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    logging.info("Writing clean orders to PostgresSQL table: %s", POSTGRES_TABLE)

    (
     clean_df.coalesce(1).write
    .format("jdbc")
    .option("url",jdbc_url)
    .option("dbtable", POSTGRES_TABLE)
    .option("user", POSTGRES_USER)
    .option("password", POSTGRES_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .option("truncate", "true")
    .save()

    )

def run() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_df = read_raw_orders(spark)
        raw_count = raw_df.count()

        logging.info("Raw orders: %s", raw_count)

        clean_df, rejected_df = transform_orders(raw_df)

        write_outputs(clean_df, rejected_df)

        write_to_postgres(clean_df)

        logging.info("Orders ETL job completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    run()

