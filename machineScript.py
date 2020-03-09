import os
import sys

machineNumber = int(sys.argv[1])
numberOfPorts = int(sys.argv[2])
numberOfMasterPorts = int(sys.argv[2])
#open heart beat process
os.system(f'python datakeeper.py 0 {machineNumber} {numberOfMasterPorts} {machineNumber} &')
for i in range(numberOfPorts):
    print(f'python datakeeper.py 1 {machineNumber*numberOfPorts+i} {numberOfMasterPorts} {machineNumber}')
    os.system(f'python datakeeper.py 1 {machineNumber*numberOfPorts+i} {numberOfMasterPorts} {machineNumber} &')

#os.system('python datakeeper.py 1 1 2 &')