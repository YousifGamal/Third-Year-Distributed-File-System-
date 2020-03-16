from multiprocessing import *
from MasterTrackerMain import *
from DNSServer import *
import sys


df = pd.DataFrame(columns=['user_id','file_name','data_node_number','file_path_on_that_data_node'
                               ,'is_data_node_alive','replicate'])


processes_number = int(sys.argv[1])
datakeeper_number = int(sys.argv[2])
machines_number = int(sys.argv[3])
needed_replications_count = int(sys.argv[4])



manager = Manager()
ns = manager.Namespace()
ns.df = df
IP_table = manager.dict()
lock = manager.Lock()
machines = manager.dict()
#initailzing the dic of lists used in the heart beat
for i in range(machines_number):
    machines[i] = manager.list()
    machines[i].append(i)
    machines[i].append(time.gmtime()[5])
    machines[i].append(1)

portsStatusList = manager.list()
#status are 'alive' 'dead' 'busy' alive also means free
for i in range(machines_number*datakeeper_number):
    portsStatusList.append('dead')

print(portsStatusList)
IP_table[-2] = True
Process(target=DNSServer, args=(IP_table, machines_number)).start()
while IP_table[-2]:
    pass
Process(target=all, args=(ns,lock,0,0,datakeeper_number,machines,portsStatusList,machines_number,IP_table,needed_replications_count)).start()
for i in range(processes_number):
    Process(target=all, args=(ns,lock,1,i,datakeeper_number,machines,portsStatusList,machines_number,IP_table,needed_replications_count)).start()


while True:
    
    print(portsStatusList)
    print(ns.df)
    time.sleep(3)
print(portsStatusList)
print(ns.df)


