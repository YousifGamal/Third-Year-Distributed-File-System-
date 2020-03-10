import zmq
import time
import sys
import pickle
import random
id = int(sys.argv[1])
masterProcessesNumber =  int(sys.argv[2])
command = sys.argv[3]
filename = sys.argv[4]

context = zmq.Context()
socket  = context.socket(zmq.REQ)

dataSocket = context.socket(zmq.REQ)
#randomiza connection to masters
masterPortsList = list(range(0,masterProcessesNumber))
random.shuffle(masterPortsList)
for i in masterPortsList:
    port = 6000+i*2
    print(port)
    socket.connect(f"tcp://192.168.43.209:{port}")

if command == 'upload':
    msg_dict = {'type':"Upload"}
    print("sent upload request")
    msg =  pickle.dumps(msg_dict)
    socket.send(msg)
    dataPort = socket.recv_string()
    print(dataPort)
    dataPort = int(dataPort)
    dataSocket.connect(f"tcp://192.168.43.177:{dataPort}")
    print(f"recieved port number {dataPort}")
    
    with open(filename,'rb') as file:
        video = file.read()
    file.close()
    video_dict = {'type':"Upload",'name': filename, 'video':video, 'id': id}
    msg = pickle.dumps(video_dict)
    dataSocket.send(msg)
    
    
    dataSocket.close()



