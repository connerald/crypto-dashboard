import requests
import time
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092')
url = 'https://data-api.binance.vision/api/v3/trades'
params = {'symbol': 'BTCUSDT', 'limit': 1}

while True:
    resp = requests.get(url, params=params)
    if resp.status_code == 200:
        data = resp.json()
        if data:
            price = float(data[0]['price'])
            ts = int(data[0]['time'])
            msg = json.dumps({'price': price, 'ts': ts})
            print(f"当前BTC价格: {price}，时间戳: {ts}")  # 新增打印
            producer.send('btc_price', msg.encode('utf8'))
    time.sleep(2)