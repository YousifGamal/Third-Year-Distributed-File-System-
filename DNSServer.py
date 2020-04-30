import zmq
import socket
import pickle
import time
import signal
import sys


machine_ip_assignment_socket = "tgf"
def signal_handler(sig, frame):
	global machine_ip_assignment_socket
	machine_ip_assignment_socket.close()
	print('You pressed Ctrl+C!')
	sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


def DNSServer(IP_table, machines_count):
	global machine_ip_assignment_socket
	#Get my IP
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(('8.8.8.8',0))  # connect() for UDP doesn't send packets
	local_ip_address = s.getsockname()[0]
	IP_table[-1] = local_ip_address
	print(local_ip_address)
	last_octet_index = local_ip_address.rfind(".")


	master_ip = "tcp://"+local_ip_address+":11050"
	#Create a push socket that assigns numbers to machines
	context = zmq.Context()
	machine_numbers_resolver_socket = context.socket(zmq.PUSH)
	machine_numbers_resolver_socket.bind(master_ip)


	for i in range(int(machines_count)):
		machine_numbers_resolver_socket.send_string(str(i))
		print("Number assigned to machine " + str(i))
		time.sleep(1)

	machine_ip_assignment_socket = context.socket(zmq.PULL)
	machine_ip_assignment_socket.bind("tcp://"+local_ip_address+":11020")






	for i in range(int(machines_count)):
		# time.sleep(5)
		msg = machine_ip_assignment_socket.recv_string()
		print("IP entry added for machine " + str(i))
		msg = msg.split()
		IP_table[int(msg[0])] = msg[1]
		print(msg)
	print(IP_table)
	machine_ip_assignment_socket.close()
	machine_numbers_resolver_socket.close()
	IP_table[-2] = False

	# while True:
	# 	time.sleep(1)

	# #socket that sends master ip
	# context = zmq.Context()
	# master_ip_socket = context.socket(zmq.PUB)
	# master_ip_socket.bind("tcp://*:9999")  # Note.


	#Socket that recieves IPs and assign them to machines

	# assign_machine_to_IPs = context.socket(zmq.PULL)
	# assign_machine_to_IPs.bind("tcp://127.0.0.1:10000")

	# while True:
	# 	try:
	# 		# master_ip_socket.send_string(local_ip_address)
	# 		msg = assign_machine_to_IPs.rec(flags=zmq.NOBLOCK)
	# 		# IP_table[]
	# 	except zmq.Again:
	# 		pass
	# 	time.sleep(1)








	# myIP = {}

	# myIP["ip"] = local_ip_address
	# msg  = pickle.dumps(myIP)
	# socket.send(msg)




