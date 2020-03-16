import os
import pickle
import random
import sys
import time

import zmq


def heartBeatHandler(number,local_ip):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    port = 9000+number*2
    socket.bind("tcp://"+local_ip+f":{port}")
    message = "I am alive"
    while True:
        data = {'Machine#':number, 'message':message}
        socket.send_pyobj(data)
        time.sleep(1)

print(sys.argv,len(sys.argv),sys.argv[5])
type = int(sys.argv[1])
number = int(sys.argv[2])
masterProcesseNumbers = int(sys.argv[3])
machineNumber = int(sys.argv[4])
dataKeeperNumberPerMachine = int(sys.argv[7])

local_ip = sys.argv[5]
master_ip = sys.argv[6]
if type == 0:
    heartBeatHandler(machineNumber,local_ip)
elif type == 1: #data keeper node
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://"+local_ip+f":{number*2+8000}")# create server port

    masterSocket = context.socket(zmq.REQ)# connect to master sockets as client
    #randomiza connection to master nodes
    masterPortsList = list(range(0,masterProcesseNumbers))

    random.shuffle(masterPortsList)
    #print(masterPortsList)

    for i in masterPortsList:
        port = 6000+i*2
        masterSocket.connect("tcp://"+master_ip+f":{port}")
    while True:
              
        msg = socket.recv()
        msg_dict = pickle.loads(msg)
        if msg_dict['type'] == "Upload":
            print("Recieved upload request from client")
            content = msg_dict
            
            print(content['name'])
            path = str(number)
            try:  
                os.mkdir(path)  
            except OSError as error:  
                pass
            path = str(number)+"/"+content['name']
            
            with open(path,"wb") as file:
                file.write(content['video'])
                file.close()
            
            print("Saved the video successfully at path = " + path)
            port = number*2+8000
            tableEntry = {'type':'Add','data':[content['id'],content['name'],machineNumber 
                                ,path, True,False,port]} 
            #print(tableEntry)
            respond = pickle.dumps(tableEntry)
            masterSocket.send(respond)
            fromMaster  = masterSocket.recv_string()
            socket.close()
            time.sleep(.1)
            socket = context.socket(zmq.REP)
            port = int(port)
            socket.bind("tcp://"+local_ip+f":{port}")

        if msg_dict['type'] == "Download":

            print("Recieved download request from client")
            with open(msg_dict['path'],'rb') as file:  # read video and be ready to send it to client
                video = file.read()
            file.close()
        
            video_dict = {'video':video}
            msg = pickle.dumps(video_dict)
            socket.send(msg)
            respond = {'type': 'Download Finished','port':number}
            respond = pickle.dumps(respond)
            masterSocket.send(respond)
            fromMaster  = masterSocket.recv_string()
        
            
            

        if msg_dict['type'] == "ReplicationDst":
            
            print("This is a replicator destination recieving from source port:",msg_dict['srcPort'])
             # socket to sub to 
            srcPort = msg_dict['srcPort']
            src_ip = msg_dict['src_ip']
            userId = msg_dict['user_id']
            fileName = msg_dict['fileName']
            context = zmq.Context()
            recieve_replica = context.socket(zmq.PAIR)
            recieve_replica.connect(f"tcp://{src_ip}:{msg_dict['srcPort']}")
            print("Waiting for the file from src")
            msg = recieve_replica.recv()
            msg = pickle.loads(msg)
            path = "rep"
            try:  
                os.mkdir(path)  
            except OSError as error:  
                pass
            
            path2 = path + "/" + str(userId) + "/"
            try:
                os.mkdir(path2)
            except OSError as error:
                pass

            with open(path2 +fileName ,"wb") as file:
                file.write(msg['video'])
            file.close()
            print("Saved the file successfully at path = " + path2)
            tableEntry = [userId, fileName, machineNumber, path2+fileName, True,True,number]
            tableEntry = pickle.dumps(tableEntry)
            socket.send(tableEntry)
            recieve_replica.close()

                    
        if msg_dict['type'] == "ReplicationSrc":
            socket.send_string("roger that")
            filePath = msg_dict['filePath']
            with open(filePath,'rb') as file:
                video = file.read()
            file.close()
            video_dict = {'video':video}
            for i in range(msg_dict['count']):
                local_number = number - machineNumber*dataKeeperNumberPerMachine
                print("This is a replicator source sending to destination no. "+str(i)+" from source port: "+str(5000+i+100*local_number))
               # socket to pub on
                context = zmq.Context()
                send_replica = context.socket(zmq.PAIR)
                send_replica.bind("tcp://"+local_ip+f":{5000+i+100*local_number}")
                msg = pickle.dumps(video_dict)
                send_replica.send(msg)
                print(f"File published to destinations from src port:{5000+i+100*local_number}")  
                send_replica.close()
