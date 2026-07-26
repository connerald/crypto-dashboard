import os

from flask import Flask, render_template
from flask_socketio import SocketIO, emit

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode='threading')

APP_HOST = os.getenv("APP_HOST", "127.0.0.1")
APP_PORT = int(os.getenv("APP_PORT", "5000"))
APP_DEBUG = os.getenv("APP_DEBUG", "false").lower() in {"1", "true", "yes", "on"}

@app.route("/")
def index():
    return render_template("index.html")

@socketio.on('btc_price')
def handle_btc_price(data):
    print("服务端收到价格数据：", data)
    emit('btc_price', data, broadcast=True)

@socketio.on('btc_alert')
def handle_btc_alert(data):
    print("服务端收到告警数据：", data)
    emit('btc_alert', data, broadcast=True)

@socketio.on('btc_change')
def handle_btc_change(data):
    print("服务端收到涨跌幅数据：", data)
    emit('btc_change', data, broadcast=True)

if __name__ == '__main__':
    socketio.run(
        app,
        debug=APP_DEBUG,
        host=APP_HOST,
        port=APP_PORT,
        allow_unsafe_werkzeug=True,
    )
