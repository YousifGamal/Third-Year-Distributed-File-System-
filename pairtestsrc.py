import zmq
context = zmq.Context()
socket = context.socket(zmq.PAIR)
socket.bind("tcp://127.0.0.1:55555")
socket.send_string("hi")
print("7a7a")