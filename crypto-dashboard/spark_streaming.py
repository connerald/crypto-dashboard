from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, expr
from pyspark.sql.types import StructType, DoubleType, LongType
from kafka import KafkaProducer
import json

KAFKA_BROKER = "localhost:9092"
PRICE_TOPIC = "btc_price"
ALERT_TOPIC = "btc_alert"
CHANGE_TOPIC = "btc_change"
THRESHOLD = 0.003  # 0.3%

# 创建SparkSession
spark = SparkSession.builder \
    .appName("BTCPriceAlert") \
    .getOrCreate()

# 定义schema
schema = StructType() \
    .add("price", DoubleType()) \
    .add("ts", LongType())

# 读取Kafka流
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", PRICE_TOPIC) \
    .load()

# 解析json
json_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data"))
price_df = json_df.select(col("data.price").alias("price"), (col("data.ts")/1000).cast("timestamp").alias("ts"))

# 计算1分钟窗口的涨跌幅
windowed = price_df.groupBy(window(col("ts"), "1 minute")).agg(
    expr("first(price) as first_price"),
    expr("last(price) as last_price")
).withColumn("change", (col("last_price") - col("first_price")) / col("first_price"))

def process_batch(df, epoch_id):
    rows = df.collect()
    for row in rows:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
        
        # 推送涨跌幅数据（无论是否超过阈值都推送）
        change_data = {
            "change": row.change,
            "change_percent": row.change * 100,
            "first_price": row.first_price,
            "last_price": row.last_price
        }
        producer.send(CHANGE_TOPIC, json.dumps(change_data).encode("utf-8"))
        
        # 如果超过阈值，推送告警
        if abs(row.change) > THRESHOLD:
            threshold_percent = THRESHOLD * 100  # 将阈值转换为百分比
            alert = {
                "alert": f"BTC价格{'上涨' if row.change > 0 else '下跌'}超{threshold_percent:.1f}%，当前价格: {row.last_price:.2f}",
                "change": row.change,
                "price": row.last_price
            }
            producer.send(ALERT_TOPIC, json.dumps(alert).encode("utf-8"))
        
        producer.close()

query = windowed.writeStream \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/btc_checkpoint") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()