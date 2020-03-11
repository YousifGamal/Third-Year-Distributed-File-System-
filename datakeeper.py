
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
dataKeeperNumberPerMachine = 2

if type == 0:
    hartBeatHandler(machineNumber)
elif type == 1: #data keeper node
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    port = number*2+8000
    print(port)
    socket.bind(f"tcp://127.0.0.1:{port}")# create server port

    masterSocket = context.socket(zmq.REQ)# connect to master sockets as client
    #randomiza connection to master nodes
    masterPortsList = list(range(0,masterProcesseNumbers))

    random.shuffle(masterPortsList)
    print(masterPortsList)

   
    """context = zmq.Context()
    send_replica = context.socket(zmq.PUB)
    send_replica.bind("tcp://127.0.0.1:4000")
    context = zmq.Context()
    recieve_replica = context.socket(zmq.SUB)
    recieve_replica.subscribe("")""" 


    for i in masterPortsList:
        portm = 6000+i*2
        masterSocket.connect(f"tcp://127.0.0.1:{portm}")
    while True:
        #print(f"data keeper with port number {port} waiting .....",port)        
        msg = socket.recv()
        msg_dict = pickle.loads(msg)
        if msg_dict['type'] == "Upload":
            print("recieved upload request from client")
            
            content = msg_dict
            
            print(content['name'])
            with open("yosry 5awal"+content['name'],"wb") as file:
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
            print(port)
            port = int(port)
            socket.bind(f"tcp://127.0.0.1:{port}")

        if msg_dict['type'] == "ReplicationDst":
            
             # socket to sub to 
            srcPort = msg_dict['srcPort']
            src_ip = msg_dict['src_ip']
            userId = msg_dict['user_id']
            fileName = msg_dict['fileName']
            context = zmq.Context()
            recieve_replica = context.socket(zmq.PAIR)
            #recieve_replica.subscribe("") 
            recieve_replica.connect(f"tcp://127.0.0.1:{5000+msg_dict['idx']}")
            print("waiting for msg from src")
            msg = recieve_replica.recv()
            msg = pickle.loads(msg)
            with open("rep/"+fileName ,"wb") as file:
                file.write(msg['video'])
            file.close()
            tableEntry = [userId, fileName, machineNumber, "rep/"+fileName, True, number]
            tableEntry = pickle.dumps(tableEntry)
            socket.send(tableEntry)
            print("sent respond to master")
            recieve_replica.close()

            
            #socket.close()
            #time.sleep(.1)
            #socket = context.socket(zmq.REP)
            #port = int(port)
            #print(port)
            #socket.bind(f"tcp://127.0.0.1:{port}")
            #print("finished")
            #masterSocket.send_string(f"done replicate send from master {port}")
            #from_master  = masterSocket.recv_string()
                    
        if msg_dict['type'] == "ReplicationSrc":
            socket.send_string("roger that")
            print("Src machines recieved from master")
            filePath = msg_dict['filePath']
            print("filePth = ", filePath)
            with open(filePath,'rb') as file:
                video = file.read()
            file.close()
            video_dict = {'video':video}
            for i in range(msg_dict['count']):
               # socket to pub on
                context = zmq.Context()
                send_replica = context.socket(zmq.PAIR)
                send_replica.bind(f"tcp://127.0.0.1:{5000+i}")
                msg = pickle.dumps(video_dict)
                send_replica.send(msg)
                
                #send_replica.send(msg)
                #send_replica.send(msg)
                #socket.send_string("dadaddadad")
                print(f"Msg published to destinations from src port:{5000+i}")  
                send_replica.close()
                #socket.close()
                #time.sleep(.1)
                #socket = context.socket(zmq.REP)
                #socket.bind(f"tcp://127.0.0.1:{port}")
                #print("finished")
                #masterSocket.send_string("done")
                
            
