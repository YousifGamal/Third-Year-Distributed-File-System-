import zmq
import time
import sys
import pickle
import random

def hartBeatHandler(number):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    port = 9000+number*2
    socket.bind(f"tcp://127.0.0.1:{port}")
    message = "I am alive"
    while True:
        data = {'Machine#':number, 'message':message}
        socket.send_pyobj(data)
        time.sleep(1)


type = int(sys.argv[1])
number = int(sys.argv[2])
masterProcesseNumbers = int(sys.argv[3])
machineNumber = int(sys.argv[4])
if type == 0:
    hartBeatHandler(machineNumber)
elif type == 1: #data keeper node
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://127.0.0.1:{number*2+8000}")# create server port

    masterSocket = context.socket(zmq.REQ)# connect to master sockets as client
    #randomiza connection to master nodes
    masterPortsList = list(range(0,masterProcesseNumbers))
    
    random.shuffle(masterPortsList)
    print(masterPortsList)

    for i in masterPortsList:
        port = 6000+i*2
        masterSocket.connect(f"tcp://127.0.0.1:{port}")
    while True:
        msg = socket.recv()
        msg_dict = pickle.loads(msg)
        if msg_dict['type'] == "Upload":
            print("recieved upload request from client")
            
            content = msg_dict
            
            print(content['name'])
            with open(str(machineNumber)+"/"+content['name'],"wb") as file:
                file.write(content['video'])
                file.close()
            port = number*2+8000
            tableEntry = {'type':'Add','data':[content['id'],content['name'],machineNumber 
                                ,content['name'], True, port]}
            print(tableEntry)
            respond = pickle.dumps(tableEntry)
            masterSocket.send(respond)
            fromMaster  = masterSocket.recv_string()
            print(fromMaster)
            
            socket.close()
            time.sleep(.1)
            socket = context.socket(zmq.REP)
            port = int(port)
            socket.bind(f"tcp://127.0.0.1:{port}")

                
            