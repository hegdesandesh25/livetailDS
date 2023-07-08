from flask import Flask, jsonify
import redis
from socket_io_emitter import Emitter
from queue import Queue

app = Flask(__name__)
r = redis.Redis()
emitter = Emitter({'host': 'localhost', 'port': 6379})

message_queue = Queue()

def listen_to_stream():
    while True:
        _, data = r.brpop('logstash')
        data = data.decode()
        print(f"Received message: {data}")
        message_queue.put(data)
        emitter.Emit('new_message', {'data': data})

@app.route('/api/logs', methods=['GET'])
def get_all_messages():
    messages = []
    while not message_queue.empty():
        message = message_queue.get()
        messages.append(message)
    return jsonify({'messages': messages})

if __name__ == '__main__':
    import threading
    stream_thread = threading.Thread(target=listen_to_stream)
    stream_thread.start()

    app.run()
