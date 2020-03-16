
from multiprocessing import *
import time
from datetime import datetime

import pandas as pd
import numpy as np

import zmq
import sys
import pickle
import random

def secondPassed(oldsecond):
    currentsecond = datetime.timestamp(datetime.now())
    if ((currentsecond - oldsecond) >= 2):
        oldsecond = currentsecond
        return True
    else:
        return False

def checkAlive(m,portsBusyList,lock,machines_number,dataKeeperNumberPerMachine,ns):
    
    if m:
        for i in range(machines_number):
            if m[i][2] == 1:
                if secondPassed(m[i][1]):
                    lock.acquire()
                    lockUpTable = ns.df
                    lockUpTable.loc[lockUpTable.data_node_number == i, 'is_data_node_alive'] = False
                    ns.df = lockUpTable
                    m[i][2] = 0
                    for j in range(dataKeeperNumberPerMachine):
                        portsBusyList[j+i*dataKeeperNumberPerMachine] = 'dead'
                    lock.release()

    return m






def add_row(inp):
    return {"user_id" : inp[0] , 'file_name' : inp[1] , 'data_node_number':inp[2],
            'file_path_on_that_data_node':inp[3],'is_data_node_alive':inp[4],'replicate':inp[5]}

def master_heart_beat(lock,ns,dataKeeperNumberPerMachine,machines,portsBusyList,machines_number,IP_table):
    ports = list()
    for i in range(machines_number):
        ports.append(9000+i*2)

    print(f"ports are {ports}")
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.subscribe("")
    socket.RCVTIMEO = 0
    for i in range(len(ports)):
        
        print("tcp://"+IP_table[i]+f":{ports[i]}")
        socket.connect("tcp://"+IP_table[i]+f":{ports[i]}")
    while True:
        try:
            work = socket.recv_pyobj()
        except zmq.error.Again:
            machines = checkAlive(machines,portsBusyList,lock,machines_number,dataKeeperNumberPerMachine,ns)
            
            continue    
            
        machineN = work['Machine#']
        message = work['message']
        
        lock.acquire()
        #declare machine and all of it ports are alive
        #for ports only declare them alive if they were dead
        machines[machineN][1] = datetime.timestamp(datetime.now())
        machines[machineN][2] = 1
        for i in range(dataKeeperNumberPerMachine):
            if portsBusyList[i+machineN*dataKeeperNumberPerMachine] == 'dead':
                portsBusyList[i+machineN*dataKeeperNumberPerMachine] = 'alive'
                lockUpTable = ns.df
                lockUpTable.loc[lockUpTable.data_node_number == machineN, 'is_data_node_alive'] = True
                ns.df = lockUpTable
        lock.release()
        machines = checkAlive(machines,portsBusyList,lock,machines_number,dataKeeperNumberPerMachine,ns)
        
    


def replicate(ns,lock,fg,proc_num,dataKeeperNumberPerMachine,machines,portsBusyList,machinesNumber,IP_table,context,replications_count):
    
    lookUpTable = ns.df
    for file in range(len(lookUpTable)):
        fileName = lookUpTable['file_name'][file]
        user_Id = lookUpTable['user_id'][file]
        lock.acquire()
        lookUpTable = ns.df
        userFile = ns.df.query('user_id == @user_Id and file_name == @fileName and is_data_node_alive == True and replicate == False')
        userFileCount = len(userFile)
        sourceMachines = userFile['data_node_number'].tolist()  # return machines numbers which have this file
        if userFileCount == 0:
            lock.release()
            continue
        sourceMachine = sourceMachines[0]
        sourceMachineFilePath = userFile['file_path_on_that_data_node'].tolist()[0]
        userId = userFile['user_id'].tolist()[0]
        if userFileCount < replications_count:
            #for i in sourceMachines:
            lookUpTable.loc[(lookUpTable.file_name == fileName) & (lookUpTable.user_id == userId), 'replicate'] = True
            ns.df = lookUpTable
            lock.release()  

            tempList = [item for item in range(0, machinesNumber)]
            dstMachines = list(set(tempList) - set(sourceMachines))
            trueDstMachines = []

            lock.acquire()
            for i in dstMachines:
                dstFile = ns.df.query('user_id == @user_Id and data_node_number == @i and file_name == @fileName and is_data_node_alive == True and replicate == False')
                if len(dstFile) == 0:
                    trueDstMachines.append()
            lock.release()
            dstMachines = trueDstMachines

            if not dstMachines:
                return
            #choose alive port to connect to
            dstDataPorts = []
            srcDataKeeperNumber = -1
            freeDsts = 0
            iterate = 0
            neededReplicasCount = replications_count - userFileCount


            print("neededReplicasCount = ", neededReplicasCount)
            
            while freeDsts < neededReplicasCount:
                if iterate >= len(dstMachines):
                    iterate = 0
                
                i = 0
                breakLoop = False
                while i < dataKeeperNumberPerMachine:
                    if i >=dataKeeperNumberPerMachine:
                        i=0
                    lock.acquire()
                    if portsBusyList[dstMachines[iterate] * dataKeeperNumberPerMachine + i] == 'alive':
                        portsBusyList[dstMachines[iterate] * dataKeeperNumberPerMachine + i] = 'busy'
                        temp = ((dstMachines[iterate] * dataKeeperNumberPerMachine + i) * 2) + 8000
                        temp = "tcp://"+IP_table[dstMachines[iterate]]+f":{temp}"
                        dstDataPorts.append(temp)
                        freeDsts += 1
                        breakLoop = True
                    lock.release()
                    if breakLoop:
                        break
                    i+=1
                
                iterate += 1
            print(dstDataPorts)
            print("hereeee")
            print(f"source machine : {sourceMachine}")
            exit = False
            srcPort = 0
            src_port = 0
            i = 0
            while not exit:
                
                for i in range(dataKeeperNumberPerMachine):
                    
                    lock.acquire()
                    
                    if portsBusyList[sourceMachine * dataKeeperNumberPerMachine + i] == 'alive':
                       
                        portsBusyList[sourceMachine * dataKeeperNumberPerMachine + i] = 'busy'
                        srcPort = ((sourceMachine * dataKeeperNumberPerMachine + i) * 2)  + 8000
                        src_port = ((sourceMachine * dataKeeperNumberPerMachine + i) * 2)  + 8000
                        srcDataKeeperNumber = i
                        srcPort = "tcp://"+IP_table[sourceMachine]+f":{srcPort}"
                        exit = True
                        lock.release()
                        break
                    lock.release()
            print(srcPort)
            print("passed")
            




            dataKeeperSocket = context.socket(zmq.REQ)
            dataKeeperSocket.connect(srcPort)
            srcData = {'type':"ReplicationSrc", 'count':len(dstDataPorts), 'filePath': sourceMachineFilePath}
            msg =  pickle.dumps(srcData)
            print("sending data to src machine.." )
            dataKeeperSocket.send(msg)
            print("data sent")
            msg = dataKeeperSocket.recv_string()
            print(msg)
            dataKeeperSocket.close()
            time.sleep(0.1)

            print("sending data to dst machine.." )


            for i in range(len(dstDataPorts)):
                dstData = {'type':"ReplicationDst", 'srcPort':5000+srcDataKeeperNumber*100+i, 'src_ip':IP_table[sourceMachine],'idx':i, 'user_id': userId, 'fileName':fileName}
                msg =  pickle.dumps(dstData)
                dataKeeperSocket = context.socket(zmq.REQ)
                dataKeeperSocket.connect(dstDataPorts[i])
                dataKeeperSocket.send(msg)
                print("waiting for replica response")
                msg = dataKeeperSocket.recv()
                msg = pickle.loads(msg)
                print(msg)
                
                print("want to take lock")
                lock.acquire()
                print("lock acquireddd")
                data = msg # get data from dictionary
                lookUpTable = ns.df
                lookUpTable = lookUpTable.append(add_row(data),ignore_index=True)
                ns.df = lookUpTable
                print("came hereeeeeeeeeee")
                #mark this port as alive
                if portsBusyList[data[6]] == 'busy':
                    print("dst busy port is now free")
                    portsBusyList[data[6]] = 'alive'
                lock.release()
                print(ns.df)
                dataKeeperSocket.close()
            print("want to take lock to free source port")
            lock.acquire()
            
            lookUpTable = ns.df
            src_port_index = (src_port-8000)//2
            print(f"lock is grnted to free port {src_port_index}")
            if portsBusyList[src_port_index]=='busy':
                print("src busy port is now free")
                portsBusyList[src_port_index]='alive'
            
            #for i in sourceMachines:
            lookUpTable.loc[(lookUpTable.file_name == fileName) & (lookUpTable.user_id == userId), 'replicate'] = False
            ns.df = lookUpTable
            lock.release()
            
        else:
            lock.release()    
        


def all(ns,lock,fg,proc_num,dataKeeperNumberPerMachine,machines,portsBusyList,machinesNumber,IP_table,needed_replications_count):
    if (fg == 1):
        datakeeper_number = dataKeeperNumberPerMachine*machinesNumber
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        port = proc_num*2+6000
        socket.bind(f"tcp://"+IP_table[-1]+f":{port}")# create server port
        socket.RCVTIMEO = 0
        # create random order of data ports
        randomPortList = list(range(0,datakeeper_number))
        random.shuffle(randomPortList)

        print(randomPortList)       
        
        while True:
            replicate(ns,lock,fg,proc_num,dataKeeperNumberPerMachine,machines,portsBusyList,machinesNumber,IP_table,context,needed_replications_count)
            try:
                msg = socket.recv()
            except zmq.error.Again:
                continue
            msg_dict = pickle.loads(msg)
            print(msg_dict['type'])
            if msg_dict['type'] == "Upload":
                print("upload request from client")
                #choose alive port to connect to
                dataPort = 0
                exit = False
                iterate = 0
                port_index = -1
                while not exit:
                    if iterate >= machinesNumber*dataKeeperNumberPerMachine:
                        iterate = 0
                    lock.acquire()
                    if portsBusyList[randomPortList[iterate]] == 'alive':
                        portsBusyList[randomPortList[iterate]] = 'busy'
                        dataPort = (randomPortList[iterate] * 2) + 8000
                        port_index = randomPortList[iterate]
                        exit = True
                    lock.release()
                    iterate += 1
                print(dataPort)
                msg = "tcp://"+IP_table[port_index//dataKeeperNumberPerMachine]+":"+str(dataPort)
                socket.send_string(msg) # send port number to client
            elif msg_dict['type']=="Add": #add to look up table
                respond = "done"
                print("recieved add request")
                socket.send_string(respond)
                print("sent respond")
                lock.acquire()
                data = msg_dict['data'] # get data from dictionary
                lookUpTable = ns.df
                lookUpTable = lookUpTable.append(add_row(data),ignore_index=True)
                ns.df = lookUpTable
                print(data)
                #mark this port as alive
                index = int((data[6] - 8000) / 2)
                if portsBusyList[index] == 'busy':
                    portsBusyList[index] = 'alive'
                lock.release()
                print(ns.df)
            
            elif msg_dict['type'] == "Download Finished":
                lock.acquire()
                if portsBusyList[msg_dict['port']] == 'busy':
                    portsBusyList[msg_dict['port']] = 'alive'
                lock.release()
                respond = "done"
                socket.send_string(respond)

            
            elif msg_dict['type'] == "Download":
                print("Download request from client")
                # check if requested file available to download
                # filtering with query method 
                user_id = msg_dict['user_id']
                file_name = msg_dict['filename']

                data = ns.df.query('user_id == @user_id and file_name == @file_name and is_data_node_alive == True')
                machine_data_found = data['data_node_number'].tolist()  # return machines numbers which have this file
                machine_data_found_paths = data['file_path_on_that_data_node'].tolist()
                print(machine_data_found,"   Data Node List")
                msg = {'status':None , 'port':None ,'path':None }
                if not machine_data_found:
                    # no data node have the requested file
                    msg['status'] = "Download Request Failed .... File Not Found"
                    msg = pickle.dumps(msg)
                    socket.send(msg)

                else:
        
                    data_dict = dict()

                    port_list_idx = []
                    for k,m in enumerate(machine_data_found):
                        data_dict[m] = machine_data_found_paths[k]
                        port_list_idx.extend([((m * dataKeeperNumberPerMachine) + i) for i in range(dataKeeperNumberPerMachine)])         # indices of available ports
                       
                        
                    print(port_list_idx)
                
                    Busy = True
                    portn = None
                    path = None
                    while Busy:
                        for idx in port_list_idx:
                            lock.acquire()
                            if portsBusyList[idx] == "alive":
                                portsBusyList[idx] = "busy"
                                temp = idx * 2 + 8000
                                print("debug_1", msg, IP_table, temp)
                                msg['path'] = data_dict[idx // dataKeeperNumberPerMachine]
                                Busy = False
                                msg['port']= "tcp://"+IP_table[idx // dataKeeperNumberPerMachine]+":"+str(temp)
                                msg['status'] = 'success'
                                break
                            lock.release()
                        lock.release()


                    print(msg['port'])
                    msg = pickle.dumps(msg)
                    socket.send(msg)
                    
                    # wait for complete download request to free port again 
 
     
    else:
        master_heart_beat(lock,ns,dataKeeperNumberPerMachine,machines,portsBusyList,machinesNumber,IP_table)
        


def test(ns,lock,fg,proc_num,dataKeeperNumberPerMachine,machines,portsBusyList,machinesNumber):
    for i in range(machinesNumber*dataKeeperNumberPerMachine):
        portsBusyList[i] = 'busy'
