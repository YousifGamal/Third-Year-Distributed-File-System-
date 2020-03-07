from multiprocessing import *
import time
import time
import pandas as pd
import numpy as np

def add_row(inp):
    return {"user id" : inp[0] , 'file name ' : inp[1] , 'data node number':inp[2],
            'file path on that data node':inp[3],'is data node alive':inp[4]}


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
       
