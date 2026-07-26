import os

from kafka import KafkaConsumer
import socketio
import threading
import json

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
SOCKETIO_URL = os.getenv("SOCKETIO_URL", "http://localhost:5000")

# 连接到本地的 SocketIO 服务
sio = socketio.Client()
sio.connect(SOCKETIO_URL)

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
            print("收到价格数据：", price)
            sio.emit('btc_price', {'price': price})
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

if __name__ == '__main__':
    threading.Thread(target=alert_consumer, daemon=True).start()
    threading.Thread(target=price_consumer, daemon=True).start()
    threading.Thread(target=change_consumer, daemon=True).start()
    import time
    while True:
        time.sleep(10)
