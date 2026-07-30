import json
import os

from kafka import KafkaProducer
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    max as spark_max,
    max_by,
    min as spark_min,
    min_by,
    window,
)
from pyspark.sql.types import DoubleType, LongType, StructType

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CHECKPOINT_LOCATION = os.getenv(
    "SPARK_CHECKPOINT_LOCATION",
    "/tmp/btc_checkpoint/rolling-60s-v1",
)
PRICE_TOPIC = "btc_price"
ALERT_TOPIC = "btc_alert"
CHANGE_TOPIC = "btc_change"

ROLLING_WINDOW_DURATION = "60 seconds"
ROLLING_WINDOW_SLIDE = "2 seconds"
WATERMARK_DELAY = "10 seconds"
THRESHOLD = 0.003

PRICE_SCHEMA = (
    StructType()
    .add("price", DoubleType())
    .add("ts", LongType())
)


def parse_prices(kafka_df: DataFrame) -> DataFrame:
    """Parse and validate price events from the Kafka source."""
    parsed = kafka_df.select(
        from_json(
            col("value").cast("string"),
            PRICE_SCHEMA,
        ).alias("data")
    )
    return (
        parsed.select(
            col("data.price").alias("price"),
            (col("data.ts") / 1000).cast("timestamp").alias("ts"),
        )
        .filter(col("price").isNotNull())
        .filter(col("ts").isNotNull())
        .filter(col("price") > 0)
    )


def calculate_rolling_changes(price_df: DataFrame) -> DataFrame:
    """Calculate event-time ordered price changes over rolling 60-second windows."""
    return (
        price_df.withWatermark("ts", WATERMARK_DELAY)
        .groupBy(
            window(
                col("ts"),
                ROLLING_WINDOW_DURATION,
                ROLLING_WINDOW_SLIDE,
            )
        )
        .agg(
            min_by(col("price"), col("ts")).alias("first_price"),
            max_by(col("price"), col("ts")).alias("last_price"),
            spark_min(col("ts")).alias("first_ts"),
            spark_max(col("ts")).alias("last_ts"),
        )
        .filter(col("last_ts") > col("first_ts"))
        .withColumn(
            "change",
            (col("last_price") - col("first_price")) / col("first_price"),
        )
    )


def select_latest_change(batch_df: DataFrame) -> Row | None:
    """Select the rolling window closest to the newest event in this batch."""
    return (
        batch_df.orderBy(
            col("last_ts").desc(),
            col("window.end").asc(),
        )
        .first()
    )


def build_change_message(row: Row) -> dict:
    return {
        "change": row.change,
        "change_percent": row.change * 100,
        "first_price": row.first_price,
        "last_price": row.last_price,
        "window_start": row.window.start.isoformat(),
        "window_end": row.window.end.isoformat(),
        "event_ts": int(row.last_ts.timestamp() * 1000),
    }


def build_alert_message(row: Row) -> dict | None:
    if abs(row.change) <= THRESHOLD:
        return None

    threshold_percent = THRESHOLD * 100
    direction = "上涨" if row.change > 0 else "下跌"
    return {
        "alert": (
            f"BTC价格{direction}超{threshold_percent:.1f}%，"
            f"当前价格: {row.last_price:.2f}"
        ),
        "change": row.change,
        "price": row.last_price,
        "event_ts": int(row.last_ts.timestamp() * 1000),
    }


def publish_batch(batch_df: DataFrame, _epoch_id: int) -> None:
    row = select_latest_change(batch_df)
    if row is None:
        return

    messages = [(CHANGE_TOPIC, build_change_message(row))]
    alert = build_alert_message(row)
    if alert is not None:
        messages.append((ALERT_TOPIC, alert))

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    try:
        for topic, message in messages:
            producer.send(topic, message).get(timeout=10)
    finally:
        producer.close()


def main() -> None:
    spark = (
        SparkSession.builder.appName("BTCPriceAlert")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", PRICE_TOPIC)
        .load()
    )
    rolling_changes = calculate_rolling_changes(parse_prices(kafka_df))
    query = (
        rolling_changes.writeStream.outputMode("update")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .foreachBatch(publish_batch)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
