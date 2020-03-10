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
    
    if m:
        for i in range(machines_number):
            if m[i][2] == 1:
                if secondPassed(m[i][1]):
                    print("Machine#: ", str(m[i][0]) + " ", "died")
                    lock.acquire()
                    m[i][2] = 0
                    for j in range(dataKeeperNumberPerMachine):
                        portsBusyList[j+i*dataKeeperNumberPerMachine] = 'dead'
                    lock.release()

    return m






def add_row(inp):
    return {"user_id" : inp[0] , 'file_name' : inp[1] , 'data_node_number':inp[2],
            'file_path_on_that_data_node':inp[3],'is_data_node_alive':inp[4]}

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
        socket.connect("tcp://"+IP_table[i]+f":{ports[i]}")
    while True:
        try:
            work = socket.recv_pyobj()
        except zmq.error.Again:
            machines = checkAlive(machines,portsBusyList,lock,machines_number,dataKeeperNumberPerMachine)
            
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
        lock.release()
        print("Subscriber received from machine#:", str(machineN) + " ", message)
        machines = checkAlive(machines,portsBusyList,lock,machines_number,dataKeeperNumberPerMachine)
        
    




def all(ns,lock,fg,proc_num,dataKeeperNumberPerMachine,machines,portsBusyList,machinesNumber,IP_table): 
    if (fg == 1):
        print("tez")
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
                msg = str(dataPort)
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
        master_heart_beat(lock,ns,dataKeeperNumberPerMachine,machines,portsBusyList,machinesNumber,IP_table)
        


def test(ns,lock,fg,proc_num,dataKeeperNumberPerMachine,machines,portsBusyList,machinesNumber):
    for i in range(machinesNumber*dataKeeperNumberPerMachine):
        portsBusyList[i] = 'busy'
