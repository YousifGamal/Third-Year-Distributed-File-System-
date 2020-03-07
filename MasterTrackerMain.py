from multiprocessing import *
import time

import pandas as pd
import numpy as np

import zmq
import random
import sys

def secondPassed(oldsecond):
    currentsecond = time.gmtime()[5]
    if ((currentsecond - oldsecond) >= 2):
        oldsecond = currentsecond
        return True
    else:
        return False

def checkAlive(m):
    dead = []
    for i in m:
        if secondPassed(m[i][1]):
            print("Machine#: ", str(m[i][0]) + " ", "died")
            dead.append(m[i][0])
    return dead

ports = ["5556", "5557"]

machines = dict()


def add_row(inp):
    return {"user id" : inp[0] , 'file name ' : inp[1] , 'data node number':inp[2],
            'file path on that data node':inp[3],'is data node alive':inp[4]}

def master_heart_beat():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.subscribe("")
    socket.RCVTIMEO = 0
    socket.connect("tcp://127.0.0.1:%s" % port1)
    while True:
        for port in ports:
            socket.connect("tcp://127.0.0.1:%s" % port)
            try:
                work = socket.recv_pyobj()
            except zmq.error.Again:
                dead = checkAlive(machines)
                for i in dead:
                    del machines[i]
                continue    

            machineN = work['Machine#']
            message = work['message']
            machines[machineN] = (machineN, time.gmtime()[5])
            #print("Subscriber received from machine#:", str(machineN) + " ", message)
            dead = checkAlive(machines)
            for i in dead:
                del machines[i]
        




def all(ns,lock,fg):
    
    if (fg == 1):
        for i in range(3):
            lock.acquire() 
            lookUpTable = ns.df
            #print(x.value)
            
            in1 = [i,'sex.mp4',2,'1.111',True]
            lookUpTable = lookUpTable.append(add_row(in1),ignore_index=True)
            
            ns.df = lookUpTable
            lock.release()
        
        #time.sleep(8)
        #print(lookUpTable)
     
    else:
        for i in range(3):
            lock.acquire() 
            lookUpTable = ns.df
            #print(x.value)
            in2 = [i,'sex.mp1',0,'-1.111',False]
            lookUpTable = lookUpTable.append(add_row(in2),ignore_index=True)
            ns.df = lookUpTable
            lock.release()
        time.sleep(7)
        lookUpTable = ns.df
        lookUpTable.loc[0]['is data node alive'] = "ddddddddd"
        ns.df = lookUpTable
       # print(lookUpTable)
       
