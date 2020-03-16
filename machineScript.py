import os
import sys
import socket
import zmq

def DNSClient(master_IP):
    #Get machine's local IP
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8',0))  # connect() for UDP doesn't send packets
    local_ip_address = str(s.getsockname()[0])
    #Get a machine number from master
    context = zmq.Context()
    socket_pull_number = context.socket(zmq.PULL)
    socket_pull_number.connect("tcp://"+master_IP+":11050")
    msg = socket_pull_number.recv_string()
    machine_number = int(msg)
    print("found it:"+msg)
    #Send my IP to the master
    context = zmq.Context()
    push_ip_socket = context.socket(zmq.PUSH)
    push_ip_socket.connect("tcp://"+master_IP+":11020")
    print(master_IP)
    push_ip_socket.send_string(str(machine_number)+" "+local_ip_address)
    push_ip_socket.close()
    socket_pull_number.close()
    return local_ip_address, machine_number

master_IP = sys.argv[1]
numberOfPorts = int(sys.argv[2])
numberOfMasterPorts = int(sys.argv[3])

local_IP,machineNumber = DNSClient(master_IP)
#open heart beat process
os.system(f'python datakeeper.py 0 {machineNumber} {numberOfMasterPorts} {machineNumber} '+ local_IP +' '+ master_IP +' '+str(numberOfPorts)+' &')
for i in range(numberOfPorts):
    print(f'python datakeeper.py 1 {machineNumber*numberOfPorts+i} {numberOfMasterPorts} {machineNumber} '+ local_IP+' '+ master_IP+ ' &')
    os.system(f'python datakeeper.py 1 {machineNumber*numberOfPorts+i} {numberOfMasterPorts} {machineNumber} '+ local_IP +' '+ master_IP+' '+str(numberOfPorts)+' &')

