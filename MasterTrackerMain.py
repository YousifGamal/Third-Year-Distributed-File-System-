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

def checkAlive(m,portsBusyList,lock,machines_number,dataKeeperNumberPerMachine):
    #m = temp_m
    if m:
        for i in range(machines_number):
            if m[i][2] == 1:
                if secondPassed(m[i][1]):
                    print("Machine#: ", str(m[i][0]) + " ", "died")
                    lock.acquire()
                    m[i][2] = 0
                    #machine = ns.df
                    #machine.loc[machine.machine_no == m[i][0],'status'] = False
                    #ns.df = machine
                    #i machine number and j is port number 
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
        socket.connect(f"tcp://127.0.0.1:{port}")
    while True:
        try:
            work = socket.recv_pyobj()
        except zmq.error.Again:
            machines = checkAlive(machines,portsBusyList,lock,machines_number,dataKeeperNumberPerMachine)
            #print(ns.df)
            continue    
            
        machineN = work['Machine#']
        message = work['message']
        #machines[machineN] = [machineN, time.gmtime()[5], 1]
        lock.acquire()
        #declare machine and all of it ports are alive
        #for ports only declare them alive if they were dead
        machines[machineN][1] = time.gmtime()[5]
        machines[machineN][2] = 1
        for i in range(dataKeeperNumberPerMachine):
            if portsBusyList[i+machineN*dataKeeperNumberPerMachine] == 'dead':
                portsBusyList[i+machineN*dataKeeperNumberPerMachine] = 'alive'
        #m = ns.df
        #m.loc[m.machine_no == machineN,'status'] = True
        #ns.df = m
        lock.release()
        print("Subscriber received from machine#:", str(machineN) + " ", message)
        machines = checkAlive(machines,portsBusyList,lock,machines_number,dataKeeperNumberPerMachine)
        #print(ns.df)
    




def all(ns,lock,fg,proc_num,dataKeeperNumberPerMachine,machines,portsBusyList,machinesNumber): 
    if (fg == 1):
        print("tez")
        #for i in range(3):
            # lock.acquire() 
            # lookUpTable = ns.df
            
            # in1 = [i,'sex.mp4',2,'1.111',True]
            # lookUpTable = lookUpTable.append(add_row(in1),ignore_index=True)
            
            # ns.df = lookUpTable
            # lock.release()
        datakeeper_number = dataKeeperNumberPerMachine*machinesNumber
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        port = proc_num*2+6000
        socket.bind(f"tcp://127.0.0.1:{port}")# create server port
        socket.RCVTIMEO = 0
        # create random order of data ports
        randomPortList = list(range(0,datakeeper_number))
        random.shuffle(randomPortList)

        print(randomPortList)
        dataKeeperSocket = context.socket(zmq.REQ)# connect to data keepers ports        
        
        while True:
            try:
                msg = socket.recv()
            except zmq.error.Again:
                continue
            msg_dict = pickle.loads(msg)
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

                
                dataKeeperSocket.connect(f"tcp://127.0.0.1:{dataPort}")
                dataKeeperSocket.send(msg) # send upload request to data keeper to get free port
                msg = dataKeeperSocket.recv_string()# recieve the port number
                print("data keeper port number "+msg)
                dataPortNumber = int(msg)
                #calculate index of the port in the port last
                index = int((dataPortNumber - 8000) /2)
                #mark as busy
                lock.acquire()
                if portsBusyList[index] == 'alive':
                    portsBusyList[index] = 'busy'
                lock.release()
                socket.send_string(msg) # send port number to client
            elif msg_dict['type']=="Add": #add to look up table
                respond = "done"
                socket.send_string(respond)
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
 
     
    else:
        master_heart_beat(lock,ns,dataKeeperNumberPerMachine,machines,portsBusyList,machinesNumber)
        


def test(ns,lock,fg,proc_num,dataKeeperNumberPerMachine,machines,portsBusyList,machinesNumber):
    for i in range(machinesNumber*dataKeeperNumberPerMachine):
        portsBusyList[i] = 'busy'
