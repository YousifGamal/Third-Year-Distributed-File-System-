
import zmq
import random
import sys
import time


machines = dict()

def secondPassed(oldsecond):
    currentsecond = time.gmtime()[5]
    if ((currentsecond - oldsecond) >= 2):
        oldsecond = currentsecond
        return True
    else:
        return False

def checkAlive(m):
    dead = []
    for i in m:
        if secondPassed(m[i][1]):
            print("Machine#: ", str(m[i][0]) + " ", "died")
            dead.append(m[i][0])
    return dead


port1 = "5556"

port2 = "5557"

ports = ["5556", "5557"]

if len(sys.argv) > 1:
    port1 =  sys.argv[1]
    port1 = int(port1)
    
    
def sub():
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
                dead = checkAlive(machines)
                for i in dead:
                    del machines[i]
                continue    

            machineN = work['Machine#']
            message = work['message']
            machines[machineN] = (machineN, time.gmtime()[5])
            print("Subscriber received from machine#:", str(machineN) + " ", message)
            dead = checkAlive(machines)
            for i in dead:
                del machines[i]
        
sub()
