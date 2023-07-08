from flask import Flask, jsonify
import redis
from socket_io_emitter import Emitter

app = Flask(__name__)
#create redis object
r=redis.Redis()

#configure emitter to connect to Redis on 6379
emitter =Emitter({'host': 'localhost', 'port':6379})
'''
#Function to listen to Redis streams
def listen_to_stream():
    while True:
        data = r.brpop('logstash')[1].decode()
       # print(f"Received message: {data}")
        emitter.Emit('new_message', {'data': data})
'''

#@app.route('/api/message', methods=['GET'])
@app.route("/api/logs",methods=['GET'])
def get_latest_message():
    #message = r.lindex('logstash', -1)
   # message=r.lrange('logstash', 0, -1)
    messages=[]
    
    while True:
     message=r.brpop('logstash')
     print("Received message", {message})
     if message is None:
            break
     messages.append(message)
    return jsonify({'messages': messages})

# Start listening to Redis Stream
#listen_to_stream()

if __name__ == '__main__':
    app.run()