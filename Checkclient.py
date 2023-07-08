import socketio

sio = socketio.Client()

@sio.event
def connect():
    print("Connected to server")

@sio.on('new_item')
def on_new_item(data):
    print('Received message: ', data)

sio.connect('http://localhost:8000')

try:
    # Keep the application running until the user decides to quit
    while True:
        pass
except KeyboardInterrupt:
    print("\nDisconnecting from server...")
    sio.disconnect()
