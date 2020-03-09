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

def checkAlive(temp_m,ns,lock):
    m = temp_m
    if m:
        for i in m:
            if m[i][2] == 1:
                if secondPassed(m[i][1]):
                    print("Machine#: ", str(m[i][0]) + " ", "died")
                    m[i][2] = 0
                    lock.acquire()
                    machine = ns.df
                    machine.loc[machine.machine_no == m[i][0],'status'] = False
                    ns.df = machine
                    lock.release()

    return m






def add_row(inp):
    return {"user_id" : inp[0] , 'file_name' : inp[1] , 'data_node_number':inp[2],
            'file_path_on_that_data_node':inp[3],'is_data_node_alive':inp[4]}

def master_heart_beat(lock,ns,datakeeper_number,machines,portsBusyList,machines_number):
    ports = []
    for i in range(machines_number):
        ports.append(9000+i*2)
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.subscribe("")
    socket.RCVTIMEO = 0
    while True:
        for port in ports:
            socket.connect(f"tcp://127.0.0.1:{port}")
            try:
                work = socket.recv_pyobj()
            except zmq.error.Again:
                machines = checkAlive(machines,ns,lock)
                print(ns.df)
                continue    
            
            machineN = work['Machine#']
            message = work['message']
            machines[machineN] = [machineN, time.gmtime()[5], 1]
            lock.acquire()
            m = ns.df
            m.loc[m.machine_no == machineN,'status'] = True
            ns.df = m
            lock.release()
            print("Subscriber received from machine#:", str(machineN) + " ", message)
            machines = checkAlive(machines,ns,lock)
            print(ns.df)
        




def all(ns,lock,fg,proc_num,datakeeper_number,machines,portsBusyList,machines_number): 
    if (fg == 1):
        print("tez")
        #for i in range(3):
            # lock.acquire() 
            # lookUpTable = ns.df
            
            # in1 = [i,'sex.mp4',2,'1.111',True]
            # lookUpTable = lookUpTable.append(add_row(in1),ignore_index=True)
            
            # ns.df = lookUpTable
            # lock.release()
        
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        port = proc_num*2+6000
        socket.bind(f"tcp://127.0.0.1:{port}")# create server port
        socket.RCVTIMEO = 0
        # connect to data keeper ports in random way
        dataKeeperPortsList = list(range(0,datakeeper_number))
        random.shuffle(dataKeeperPortsList)

        dataKeeperSocket = context.socket(zmq.REQ)# connect to data keepers ports
        for i in dataKeeperPortsList:
            port = i*2+8000
            dataKeeperSocket.connect(f"tcp://127.0.0.1:{port}")
        
        
        while True:
            try:
                msg = socket.recv()
            except zmq.error.Again:
                continue
            msg_dict = pickle.loads(msg)
            if msg_dict['type'] == "Upload":
                print("upload request from client")
                dataKeeperSocket.send(msg) # send upload request to data keeper to get free port
                msg = dataKeeperSocket.recv_string()# recieve the port number
                print("data keeper port number "+msg)
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
                lock.release()
                print(ns.df)
 
     
    else:
        master_heart_beat(lock,ns,datakeeper_number,machines,portsBusyList,machines_number)
        
