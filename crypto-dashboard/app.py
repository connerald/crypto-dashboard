from flask import Flask, render_template
from flask_socketio import SocketIO, emit

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode='eventlet')

@app.route("/")
def index():
    return render_template("index.html")

@socketio.on('btc_price')
def handle_btc_price(data):
    print("服务端收到价格数据：", data)
    emit('btc_price', data, broadcast=True)

if __name__ == '__main__':
    socketio.run(app, debug=True, host="127.0.0.1", port=5000)