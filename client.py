import os
import pickle
import random
import sys
import time

import zmq

id = int(sys.argv[1])
masterProcessesNumber =  int(sys.argv[2])
command = sys.argv[3]
filename = sys.argv[4]
master_IP = sys.argv[5]

context = zmq.Context()
socket  = context.socket(zmq.REQ)

dataSocket = context.socket(zmq.REQ)
#randomiza connection to masters
masterPortsList = list(range(0,masterProcessesNumber))
random.shuffle(masterPortsList)
for i in masterPortsList:
    port = 6000+i*2
    print(port)
    socket.connect(f"tcp://"+master_IP+f":{port}")

if command == 'upload':
    msg_dict = {'type':"Upload"}
    msg =  pickle.dumps(msg_dict)
    socket.send(msg)
    print("Sent upload request")
    dataPort = socket.recv_string()
    
    dataSocket.connect(dataPort)
    print("Recieved port number :  "+dataPort)
    
    with open(filename,'rb') as file:
        video = file.read()
    file.close()
    video_dict = {'type':"Upload",'name': filename, 'video':video, 'id': id}
    msg = pickle.dumps(video_dict)
    dataSocket.send(msg)
    dataSocket.close()
    print("Upload finished")


if command == 'download':
    msg_dict = {'type':"Download",'user_id':id,'filename':filename}
    msg =  pickle.dumps(msg_dict)
    socket.send(msg)
    print("Sent Download request")

    msg = socket.recv()
    msg = pickle.loads(msg)

    if msg['status'] == "Download Request Failed .... File Not Found":
        # failed Request 
        print(msg['status'] + " Make sure of file name ")
    else:
         
        msg_to_dk = {'type':"Download",'path':msg['path']}
        dataPort = msg['port']
        dataSocket.connect(dataPort)
        print(f"Recieved port number {dataPort} to Begin download")
        msg = pickle.dumps(msg_to_dk)  # Send to data node to begin download
        dataSocket.send(msg)

        msg = dataSocket.recv()       # recieve video from datanode and save it on my pc
        content = pickle.loads(msg)
       
        path = 'client'
        try:  
            os.mkdir(path)  
        except OSError as error:  
            pass

        with open("client/" + filename ,'wb') as file:
            file.write(content['video'])
        file.close()
        print("Download finished")
        
    dataSocket.close()
