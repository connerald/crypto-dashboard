import json
import os
import threading
import time

from kafka import KafkaConsumer
import socketio

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
SOCKETIO_URL = os.getenv("SOCKETIO_URL", "http://localhost:5000")
SOCKETIO_RETRY_DELAY_SECONDS = 2

sio = socketio.Client(
    reconnection=True,
    reconnection_attempts=0,
)


def connect_socketio():
    while True:
        try:
            sio.connect(SOCKETIO_URL)
            print(f"已连接 Socket.IO 服务：{SOCKETIO_URL}")
            return
        except socketio.exceptions.ConnectionError as error:
            print(
                f"连接 Socket.IO 服务失败：{error}；"
                f"{SOCKETIO_RETRY_DELAY_SECONDS} 秒后重试"
            )
            time.sleep(SOCKETIO_RETRY_DELAY_SECONDS)


def alert_consumer():
    consumer = KafkaConsumer(
        'btc_alert',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    )
    for msg in consumer:
        try:
            # 解析 JSON 数据
            alert_data = json.loads(msg.value.decode('utf8'))
            alert_message = alert_data.get('alert')  # 提取中文消息
            print("收到告警数据：", alert_message)
            sio.emit('btc_alert', {'data': alert_message})  # 只发送中文消息
        except Exception as e:
            print("解析告警数据出错：", e)

def price_consumer():
    consumer = KafkaConsumer(
        'btc_price',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    )
    for msg in consumer:
        try:
            data = json.loads(msg.value.decode('utf8'))
            price = data.get('price')
            timestamp = data['ts']
            print("收到价格数据：", price)
            sio.emit('btc_price', {'price': price, 'ts': timestamp})
        except Exception as e:
            print("解析价格数据出错：", e)

def change_consumer():
    consumer = KafkaConsumer(
        'btc_change',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    )
    for msg in consumer:
        try:
            data = json.loads(msg.value.decode('utf8'))
            change_percent = data.get('change_percent')
            print("收到涨跌幅数据：", f"{change_percent:.2f}%")
            sio.emit('btc_change', {'change_percent': change_percent})
        except Exception as e:
            print("解析涨跌幅数据出错：", e)


def main():
    connect_socketio()
    for target in (alert_consumer, price_consumer, change_consumer):
        threading.Thread(target=target, daemon=True).start()
    sio.wait()


if __name__ == '__main__':
    main()
