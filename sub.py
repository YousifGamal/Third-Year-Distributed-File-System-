
import zmq
import random
import sys
import time
from collections import defaultdict
machines = defaultdict(lambda : None)

# machines = dict()

def secondPassed(oldsecond):
    currentsecond = time.gmtime()[5]
    if ((currentsecond - oldsecond) >= 2):
        oldsecond = currentsecond
        return True
    else:
        return False

def checkAlive(temp_m):
    m = temp_m
    if m:
        for i in m:
            if m[i][2] == 1:
                if secondPassed(m[i][1]):
                    print("Machine#: ", str(m[i][0]) + " ", "died")
                    m[i][2] = 0

    return m


port1 = "5556"

port2 = "5557"

ports = ["5556", "5557"]

if len(sys.argv) > 1:
    port1 =  sys.argv[1]
    port1 = int(port1)
    
    
def sub(machines):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.subscribe("")
    socket.RCVTIMEO = 0
    socket.connect("tcp://127.0.0.1:%s" % port1)
    while True:
        for port in ports:
            socket.connect("tcp://127.0.0.1:%s" % port)
            try:
                work = socket.recv_pyobj()
            except zmq.error.Again:
                machines = checkAlive(machines)
                continue    
            
            machineN = work['Machine#']
            message = work['message']
            machines[machineN] = [machineN, time.gmtime()[5], 1]
            print("Subscriber received from machine#:", str(machineN) + " ", message)
            machines = checkAlive(machines)
        
sub(machines)
