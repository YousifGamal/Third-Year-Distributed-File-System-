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
if type == 0:
    hartBeatHandler(number)
elif type == 1: #data keeper node
    number = int(sys.argv[2])
    masterProcesseNumbers = int(sys.argv[3])
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
            print("recieved upload request from master")
            port = number*2+8000
            port = str(port)
            socket.send_string(port)
            print("sent port number is "+port)
            msg = socket.recv()
            #toClient = "I recieved your msg"
            #socket.send_string(toClient)
            socket.close()
            time.sleep(.1)
            socket = context.socket(zmq.REP)
            print(number)
            socket.bind(f"tcp://127.0.0.1:{number*2+8000}")
            print("i came here")
            content = pickle.loads(msg)
            print(content['name'])
            #with open(content['name'],"wb") as file:
            with open(str(number)+"/"+content['name'],"wb") as file:
                file.write(content['video'])
                file.close()
            tableEntry = {'type':'Add','data':[content['id'],content['name'],number 
                                ,str(number)+"/"+content['name'], True]}
            print(tableEntry)
            respond = pickle.dumps(tableEntry)
            masterSocket.send(respond)
            fromMaster  = masterSocket.recv_string()
            print(fromMaster)

        if msg_dict['type'] == "Download":
             with open(str(number)+"/"+msg_dict['filename'],'rb') as file:  # read video and be ready to send it to client
                video = file.read()
             file.close()
        
            video_dict = {'filename': msg_dict['filename'], 'video':video}
            msg = pickle.dumps(video_dict)
            socket.send(msg) 
        
            
                
            