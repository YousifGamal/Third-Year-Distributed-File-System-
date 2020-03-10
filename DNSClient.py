import socket
import zmq
import signal
import sys


master_IP = sys.argv[1]


s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(('8.8.8.8',0))  # connect() for UDP doesn't send packets
local_ip_address = str(s.getsockname()[0])
last_octet_index = local_ip_address.rfind(".")
# print("tcp://"+local_ip_address[:last_octet_index+1]+str(2)+":11000")
# msg = {}
# msg =socket.recv()
# msg = msg.loads(msg)
# master_IP = ""
# machine_number = -1
# master_not_found = True
context = zmq.Context()
socket_pull_number = context.socket(zmq.PULL)



socket_pull_number.connect("tcp://"+master_IP+":11050")
# socket_pull_number.connect("tcp://192.168.43.83:11000")


def signal_handler(sig, frame):
	global socket_pull_number
	socket_pull_number.close()
	print('You pressed Ctrl+C!')
	sys.exit(0)



signal.signal(signal.SIGINT, signal_handler)


msg = socket_pull_number.recv_string()
# msg = socket_pull_number.recv_string()

machine_number = int(msg)
print("found it:"+msg)

context = zmq.Context()
push_ip_socket = context.socket(zmq.PUSH)
push_ip_socket.connect("tcp://"+master_IP+":11020")
print(master_IP)
push_ip_socket.send_string(str(machine_number)+" "+local_ip_address)


# while master_not_found:
# 	for i in range(255):
		
		
# 		i = 83
# 		trial_master_ip = "tcp://"+local_ip_address[:last_octet_index+1]+str(i)+":11050"
# 		print(str(i)+" "+trial_master_ip)
# 		socket_pull_number.connect(trial_master_ip)
# 		# socket_pull_number.connect("tcp://192.168.43.83:11000")
# 		try:
# 			msg = socket_pull_number.recv_string(flags=zmq.NOBLOCK)
# 			# msg = socket_pull_number.recv_string()

# 			master_not_found = False
# 			machine_number = int(msg)
# 			master_IP = str(local_ip_address[:last_octet_index+1]+str(i))
# 			print("found it:"+master_IP)
# 			break

# 		except zmq.Again:
# 			# socket_pull_number.close()
# 			print("not found")
# 			pass
# print (master_IP)





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




