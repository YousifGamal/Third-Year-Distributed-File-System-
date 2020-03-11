machine_num = 4
ports_num = 2
machine_data_found = [0]
machine_Status = ["busy","busy","busy","free","busy","busy","busy","free"]

# Generate machines ports number
ports = []
port_list_idx = []
for m in machine_data_found:
    port_list_idx.extend([((m * ports_num) + i) for i in range(ports_num)])         # indices of available ports
    ports.extend([ ((m * ports_num) + i) * 2 +8000 for i in range(ports_num)])      # ports of corresponding Indices
    
print(port_list_idx)
print(ports)

Busy = True
portn = None
while Busy:
    for idx in port_list_idx:
        if machine_Status[idx] == "free":
            machine_Status[idx] = "busy"
            Busy = False
            portn = idx * 2 +8000
            break

print(portn)
print(machine_Status)

    

