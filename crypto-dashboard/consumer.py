from kafka import KafkaConsumer
import socketio
import threading
import json

# 连接到本地的 SocketIO 服务
sio = socketio.Client()
sio.connect('http://localhost:5000')

def alert_consumer():
    consumer = KafkaConsumer('btc_alert')
    for msg in consumer:
        alert = msg.value.decode('utf8')
        print("收到告警数据：", alert)
        sio.emit('btc_alert', {'data': alert})

def price_consumer():
    consumer = KafkaConsumer('btc_price')
    for msg in consumer:
        try:
            data = json.loads(msg.value.decode('utf8'))
            price = data.get('price')
            print("收到价格数据：", price)
            sio.emit('btc_price', {'price': price})
        except Exception as e:
            print("解析价格数据出错：", e)

if __name__ == '__main__':
    threading.Thread(target=alert_consumer, daemon=True).start()
    threading.Thread(target=price_consumer, daemon=True).start()
    import time
    while True:
        time.sleep(10)