import redis
import time
import atexit
import socketio
import concurrent.futures

# Set up a connection pool
pool = redis.ConnectionPool(host='localhost', port=6379, db=0)

# Create a Redis connection
r = redis.Redis(connection_pool=pool)

# Set a variable for batch size
Log_stream_size = 100

# Dictionary to store the subscribed streams
subscribed_streams = {}

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
                    item = message[b'message'].decode('utf-8')  # Extract the message
                    #print(f"Emitting message: {item.decode('utf-8')}")
                    # Emit the item to all connected Socket.IO clients
                    io.emit('new_item', item)
                   # io.emit('new_item', item.decode('utf-8'))
    except redis.exceptions.RedisError as e:
        print(f"Error reading items from stream: {e}")

'''
'''
def lift_shift_items(key):
    try:
        items = r.lrange(key, 0, Log_stream_size - 1)
        if items:
            pipeline = r.pipeline()
            for item in items:
                pipeline.xadd(key, {'message': item})
                io.emit('new_item', item.decode('utf-8'))
            pipeline.ltrim(key, len(items) - 1)
            pipeline.execute()
    except redis.exceptions.RedisError as e:
        print(f"Error moving items from list to stream for key {key}: {e}")
'''
def move_to_stream(key):
    try:
        
        stream_key = key.decode('utf-8') + "-stream"
        pipeline = r.pipeline()
        items = r.lrange(key, 0, Log_stream_size-1)
        if items:
            for item in items:
                # Write the item to the stream
                pipeline.xadd(stream_key, {'message': item})
                # Emit the item to all connected Socket.IO clients
                io.emit('new_item', item.decode('utf-8'))
            # Remove the processed items from the list
            pipeline.ltrim(key, 0, -1)
            pipeline.execute()
    except redis.exceptions.RedisError as e:
        print(f"Error moving items from list to stream: {e}")


@io.event
def subscribe_stream(sid, stream):
    print(f"Received subscription request for stream: {stream}")
    room = stream + '-stream'
    io.enter_room(sid, room)
    if room not in subscribed_streams:
        subscribed_streams[room] = set()
    subscribed_streams[room].add(sid)
    print(f"Client {sid} subscribed to stream: {stream}")

@io.event
def unsubscribe_stream(sid, stream):
    print(f"Received unsubscription request for stream: {stream}")
    room = stream + '-stream'
    io.leave_room(sid, room)
    if room in subscribed_streams and sid in subscribed_streams[room]:
        subscribed_streams[room].remove(sid)
    print(f"Client {sid} unsubscribed from stream: {stream}")

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

'''
# Define a function to be called on exit
def on_exit():
    print("Gracefully shutting down...")
   # move_items()  # Move any remaining items
  # lift_shift_items()

atexit.register(on_exit)

if __name__ == '__main__':
    # Start the Socket.IO server in a separate thread
    import eventlet.wsgi
    import eventlet
    eventlet.monkey_patch()

    server = eventlet.listen(('0.0.0.0', 8000))
    eventlet.spawn(eventlet.wsgi.server, server, app)

    #while True:
       # keys=list(r.scan_iter())
       # for key in keys:
       #     lift_shift_items(key)
       #     time.sleep(0.01) 

    with concurrent.futures.ThreadPoolExecutor() as executor:
        while True:
            keys = list(r.scan_iter())
            futures = []
            for key in keys:
                #print(r.type(key))
                if r.type(key).decode('utf-8') == 'list':
                   # print("entered type list")
                    future = executor.submit(move_to_stream, key)
                    futures.append(future)
            concurrent.futures.wait(futures)
'''
# Start the Socket.IO server in a separate thread
import eventlet.wsgi
eventlet.wsgi.server(eventlet.listen(('0.0.0.0', 8000)), app)

while True:
    move_items()
    time.sleep(0.01)  # Sleep for a bit to avoid busy-waiting
'''