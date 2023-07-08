import redis
import time
import atexit
import socketio

# Set up a connection pool
pool = redis.ConnectionPool(host='localhost', port=6379, db=0)

# Create a Redis connection
r = redis.Redis(connection_pool=pool)

# Set a variable for batch size
Log_stream_size = 100

# Set up a Socket.IO server
io = socketio.Server(cors_allowed_origins='*', debug=True)
app = socketio.WSGIApp(io,static_files={'/': {'content_type': 'text/html', 'filename': 'index.html'}})

# Define event handlers
@io.event
def connect(sid, environ):
    print('connect', sid)

@io.event
def my_message(sid, data):
    print('message', data)

@io.event
def disconnect(sid):
    print('disconnect', sid)

'''
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
# Define a function to move items from list to stream and emit them
def move_items():
    try:
        pipeline = r.pipeline()
        items = r.lrange('logstash', 0, Log_stream_size-1)
        if items:
            for item in items:
                # Write the item to the stream
                pipeline.xadd('logstash-stream', {'message': item})
                # Emit the item to all connected Socket.IO clients
                io.emit('new_item', item.decode('utf-8'))
            # Remove the processed items from the list
            pipeline.ltrim('logstash', len(items), -1)
            pipeline.execute()
    except redis.exceptions.RedisError as e:
        print(f"Error moving items from list to stream: {e}")


# Define a function to be called on exit
def on_exit():
    print("Gracefully shutting down...")
    move_items()  # Move any remaining items

atexit.register(on_exit)

# Start the Socket.IO server in a separate thread
import eventlet.wsgi
eventlet.wsgi.server(eventlet.listen(('0.0.0.0', 8000)), app)

while True:
    move_items()
    time.sleep(0.01)  # Sleep for a bit to avoid busy-waiting
