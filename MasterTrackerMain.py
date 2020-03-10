from multiprocessing import *
import time

import pandas as pd
import numpy as np

import zmq
import random
import sys
import pickle
import random

def secondPassed(oldsecond):
    currentsecond = time.gmtime()[5]
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
                    print("Machine#: ", str(m[i][0]) + " ", "died")
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
            'file_path_on_that_data_node':inp[3],'is_data_node_alive':inp[4]}

def master_heart_beat(lock,ns,dataKeeperNumberPerMachine,machines,portsBusyList,machines_number):
    ports = list()
    for i in range(machines_number):
        ports.append(9000+i*2)

    print(f"ports are {ports}")
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.subscribe("")
    socket.RCVTIMEO = 0
    for port in ports:
        socket.connect(f"tcp://192.168.43.177:{port}")
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
        machines[machineN][1] = time.gmtime()[5]
        machines[machineN][2] = 1
        for i in range(dataKeeperNumberPerMachine):
            if portsBusyList[i+machineN*dataKeeperNumberPerMachine] == 'dead':
                portsBusyList[i+machineN*dataKeeperNumberPerMachine] = 'alive'
                lockUpTable = ns.df
                lockUpTable.loc[lockUpTable.data_node_number == machineN, 'is_data_node_alive'] = True
                ns.df = lockUpTable
        lock.release()
        #print("Subscriber received from machine#:", str(machineN) + " ", message)
        machines = checkAlive(machines,portsBusyList,lock,machines_number,dataKeeperNumberPerMachine,ns)
        
    




def all(ns,lock,fg,proc_num,dataKeeperNumberPerMachine,machines,portsBusyList,machinesNumber): 
    if (fg == 1):
        print("tez")
        datakeeper_number = dataKeeperNumberPerMachine*machinesNumber
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        port = proc_num*2+6000
        socket.bind(f"tcp://192.168.43.209:{port}")# create server port
        socket.RCVTIMEO = 0
        # create random order of data ports
        randomPortList = list(range(0,datakeeper_number))
        random.shuffle(randomPortList)

        print(randomPortList)
        #dataKeeperSocket = context.socket(zmq.REQ)# connect to data keepers ports        
        
        while True:
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
                while not exit:
                    if iterate >= machinesNumber*dataKeeperNumberPerMachine:
                        iterate = 0
                    lock.acquire()
                    if portsBusyList[randomPortList[iterate]] == 'alive':
                        portsBusyList[randomPortList[iterate]] = 'busy'
                        dataPort = (randomPortList[iterate] * 2) + 8000
                        exit = True
                    lock.release()
                    iterate += 1
                print(dataPort)
                msg = str(dataPort)
                socket.send_string(msg) # send port number to client
            elif msg_dict['type']=="Add": #add to look up table
                respond = "done"
                print("recieved add request")
                socket.send_string(respond)
                print("sent respond")
                lock.acquire()
                data = msg_dict['data'] # get data from dictionary(
                print(data)
                lookUpTable = ns.df
                lookUpTable = lookUpTable.append(add_row(data),ignore_index=True)
                ns.df = lookUpTable
                #mark this port as alive
                index = int((data[5] - 8000) / 2)
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
                    #machine_num = 4
                    #ports_num = 2
                    #machine_data_found = [0]
                    #machine_Status = ["busy","busy","busy","free","busy","busy","busy","free"] # to simulate

                    # Generate machines ports number
                    ports = []
                    port_list_idx = []
                    for m in machine_data_found:
                        port_list_idx.extend([((m * dataKeeperNumberPerMachine) + i) for i in range(dataKeeperNumberPerMachine)])         # indices of available ports
                        #ports.extend([ ((m * ports_num) + i) * 2 +8000 for i in range(ports_num)])      # ports of corresponding Indices
                        
                    print(port_list_idx)
                    print(ports)

                    Busy = True
                    portn = None
                    path = None
                    while Busy:
                        for idx in port_list_idx:
                            lock.acquire()
                            if portsBusyList[idx] == "alive":
                                portsBusyList[idx] = "busy"
                                msg['path'] = machine_data_found_paths[idx // dataKeeperNumberPerMachine]
                                Busy = False
                                msg['port']= str(idx * 2 +8000)
                                msg['status'] = 'success'
                                break
                        lock.release()


                    print(msg['port'])
                    msg = pickle.dumps(msg)
                    socket.send(msg)
                    
                    # wait for complete download request to free port again 
 
     
    else:
        master_heart_beat(lock,ns,dataKeeperNumberPerMachine,machines,portsBusyList,machinesNumber)
        


def test(ns,lock,fg,proc_num,dataKeeperNumberPerMachine,machines,portsBusyList,machinesNumber):
    for i in range(machinesNumber*dataKeeperNumberPerMachine):
        portsBusyList[i] = 'busy'
