
import zmq
import random
import sys
import time

port = "5556"


Number = "1"

message = "I am alive"

if len(sys.argv) > 1:
    Number =  sys.argv[1]
    Number = int(Number)
    
if len(sys.argv) > 2:
    port =  sys.argv[2]
    port = int(port)    

def pub():
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:%s" % port)
    while True:
        data = {'Machine#':Number, 'message':message}
        socket.send_pyobj(data)
        time.sleep(1)
    
pub()   
