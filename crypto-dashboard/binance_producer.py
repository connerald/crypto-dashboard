import json
import os
import time

import requests
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
BINANCE_TRADES_URL = "https://data-api.binance.vision/api/v3/trades"
BINANCE_TRADES_PARAMS = {"symbol": "BTCUSDT", "limit": 1}
REQUEST_TIMEOUT = (3.05, 10)
POLL_INTERVAL_SECONDS = 2
PRICE_TOPIC = "btc_price"


def publish_latest_trade(producer):
    try:
        response = requests.get(
            BINANCE_TRADES_URL,
            params=BINANCE_TRADES_PARAMS,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
        if not data:
            print("Binance 未返回最新成交数据")
            return False

        price = float(data[0]["price"])
        timestamp = int(data[0]["time"])
    except (requests.RequestException, KeyError, IndexError, TypeError, ValueError) as error:
        print(f"获取 Binance 行情失败：{error}")
        return False

    message = json.dumps({"price": price, "ts": timestamp})
    print(f"当前BTC价格: {price}，时间戳: {timestamp}")
    producer.send(PRICE_TOPIC, message.encode("utf8"))
    return True


def run(producer):
    while True:
        publish_latest_trade(producer)
        time.sleep(POLL_INTERVAL_SECONDS)


def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    try:
        run(producer)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
