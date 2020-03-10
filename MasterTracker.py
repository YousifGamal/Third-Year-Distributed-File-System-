from multiprocessing import *
from MasterTrackerMain import *
import sys


df = pd.DataFrame(columns=['user_id','file_name','data_node_number','file_path_on_that_data_node'
                               ,'is_data_node_alive'])

#machines = pd.DataFrame(columns=['machine_no', 'status'])
#machines = machines.append({'machine_no':0, 'status':False},ignore_index=True)
#machines = machines.append({'machine_no':1, 'status':False},ignore_index=True)

processes_number = int(sys.argv[1])
datakeeper_number = int(sys.argv[2])
machines_number = int(sys.argv[3])


manager = Manager()
ns = manager.Namespace()
ns.df = df

lock = manager.Lock()
machines = manager.dict()
portsBusyList = manager.list()
for i in range(processes_number):
    Process(target=all, args=(ns,lock,1,i,datakeeper_number,machines,portsBusyList,machines_number)).start()
#Process(target=all, args=(ns, lock,0)).start()
time.sleep(100)
print(ns.df)


