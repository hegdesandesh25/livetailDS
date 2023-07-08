from flask import Flask, jsonify
import redis
import socketio
from socket_io_emitter import Emitter
from queue import Queue
import atexit
import time
import eventlet.wsgi

#setup connection pool
constr = redis.ConnectionPool(host='localhost',port=6379,db=0)

# Connect to redis
r= redis.Redis(connection_pool=constr)

#set no of log-streams to process
Log_stream_size=100

# Set up a Socket.IO server
io = socketio.Server(cors_allowed_origins='*')

# Set up a Flask app and a Socket.IO server
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins='*')

def move_items():
    try:
        # Use XREAD to read from the stream
        items = r.xread({ 'logstash-stream': '0' }, count=Log_stream_size, block=0)
        if items:
            for stream, message_list in items:
                for id, message in message_list:
                    item = message[b'message']  # Extract the message
                    #print(f"Emitting message: {item.decode('utf-8')}")
                    # Emit the item to all connected Socket.IO clients
                    io.emit('new_item', item.decode('utf-8'))
    except redis.exceptions.RedisError as e:
        print(f"Error reading items from stream: {e}")

'''

#Function to move items from list to stream
def move_items():
    try:
        pipeline = r.pipeline()
        items = r.lrange('logstash',0,Log_stream_size-1)
        if items:
            for item in items:
                #write to stream 
                pipeline.xadd('logstash-stream',{'message': item})
                #print(item)
                #Emit the item to all connected socket.io client
                io.emit('new_item',item.decode('utf-8'))
                
            #remove processed item from list
            pipeline.ltrim('logstash',len(items),-1)
            pipeline.execute()
    except redis.exceptions.RedisError as e:
        print(f"Error moving items from list to stream: {e}")

'''
# Define a function to be called on exit
def on_exit():
    print(" shutting down...")
    move_items()  # Move any remaining items

atexit.register(on_exit)

# Start the Socket.IO server in a separate thread
#import eventlet.wsgi
#eventlet.spawn(eventlet.wsgi.server, eventlet.listen(('', 8000)), app)
eventlet.spawn(eventlet.wsgi.server, eventlet.listen(('127.0.0.1', 8000)), app)

'''
while True:
    move_items()
    time.sleep(0.01)  # Sleep for a bit to avoid busy-waiting
    '''
if __name__ == '__main__':
    # Start a loop that keeps reading from the stream and emitting events
    def loop():
        while True:
            move_items()
            time.sleep(0.01)  # Sleep for a bit to avoid busy-waiting

    # Run the loop in a separate thread
    socketio.start_background_task(loop)

    # Start the Flask+Socket.IO app
    socketio.run(app, host='127.0.0.1', port=8000)    