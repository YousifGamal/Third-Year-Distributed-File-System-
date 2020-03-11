import subprocess 
available = []
for ping in range(0,255): 
	address = "192.168.43." + str(ping) 
	res = subprocess.call(['fping', '-c', '1', address]) 
	if res == 0: 
		available.append(ping)
print(available)