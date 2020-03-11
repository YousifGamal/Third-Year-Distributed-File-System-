
import zmq
import time
import sys
import pickle
import random

number = int(sys.argv[1])

def replication():
    context = zmq.Context()
    # socket to sub to master for replication
    master_replica = context.socket(zmq.SUB)
    master_replica.subscribe("")
    master_replica.connect("tcp://127.0.0.1:4000")
    # socket to sub to 
    recieve_replica = context.socket(zmq.SUB)
    recieve_replica.subscribe("")
    # socket to pub on
    send_replica = context.socket(zmq.PUB)
    port  = number * 2 + 5000
    send_replica.bind("tcp://*:%s" % str(port))

    while True:
        msg = master_replica.recv_pyobj()
        if msg['type'] == "Replication":
            if msg['src'] == number:
                print("Src machine recieved from master")
                time.sleep(.3)
                data = {'msg': "Msg to destinations"}
                send_replica.send_pyobj(data)
                print("Msg published to destinations from src port:"+str(port))
            else:
                print(msg)
                srcPort = msg['src'] * 2 + 5000
                print(srcPort)
                src_ip = msg['ip'] 
                print(src_ip)
                recieve_replica.connect("tcp://"+src_ip+':'+ str(srcPort))
                print("waiting for msg from src")
                msg = recieve_replica.recv_pyobj()
                print (msg)
replication()                
