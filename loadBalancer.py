import socket, threading, sys, time

LISTENING_PORT = 80
LOCK = threading.Lock()
LB_SERVER_ADDRESS = '10.0.0.1'
CLIENTS = {}
SERVERS = {'serv1' : {'M':2, 'P':1, 'V':1, 'time':0, 'address': '192.168.0.101', 'socket' : None},
           'serv2' : {'M':2, 'P':1, 'V':1, 'time':0, 'address': '192.168.0.102', 'socket' : None},
           'serv3' : {'M':1, 'P':2, 'V':3, 'time':0, 'address': '192.168.0.103', 'socket' : None}}


def LBLogger(msg):
    print '{time}: {msg}-----\n'.format(
        time=time.strftime('%H:%M:%S', time.localtime(time.time())),
        msg = msg)


class ClientThread(threading.Thread):

    def __init__(self, client_address, client_socket):
        threading.Thread.__init__(self)
        self.client_socket = client_socket
        self.client_address = client_address

    def run(self):
        request = self.client_socket.recv(2)
        request_type = request[0]
        request_len = request[1]
        server_name = getBestServer(self.client_address, request_type, request_len)
        LBLogger('recieved request {request} from {client_address}, sending to {server_address}'.format(
                request=request,
                client_address=self.client_address[0],
                server_address=SERVERS[server_name]['address']))
        server_socket = SERVERS[server_name]['socket']
        server_socket.sendall(request)
        data = server_socket.recv(2)
        self.client_socket.sendall(data)
        self.client_socket.close()

def update_servers_time(current_time):
    SERVERS['serv1']['time'] = max(current_time, SERVERS['serv1']['time'])
    SERVERS['serv2']['time'] = max(current_time, SERVERS['serv2']['time'])
    SERVERS['serv3']['time'] = max(current_time, SERVERS['serv3']['time'])


def getBestServer(client_address, request_type, request_len):
    global LOCK
    LOCK.acquire()
    request_len = int(request_len)
    if client_address in CLIENTS:
        current_time = CLIENTS[client_address]
    else: # First request of a client is in TIME=0
        current_time = 0

    update_servers_time(current_time)

    servers_task_time = {}
    servers_task_time['serv1'] = SERVERS['serv1'][request_type] * request_len
    servers_task_time['serv2'] = SERVERS['serv2'][request_type] * request_len
    servers_task_time['serv3'] = SERVERS['serv3'][request_type] * request_len

    serv1_task_end_time = servers_task_time['serv1'] + SERVERS['serv1']['time']
    serv2_task_end_time = servers_task_time['serv2'] + SERVERS['serv2']['time']
    serv3_task_end_time = servers_task_time['serv3'] + SERVERS['serv3']['time']


    if serv1_task_end_time < serv2_task_end_time:
        if serv1_task_end_time < serv3_task_end_time:
            best_server = 'serv1'
        else:
            best_server = 'serv3'
    elif serv2_task_end_time < serv3_task_end_time:
        best_server = 'serv2'
    else:
        best_server = 'serv3'

    SERVERS[best_server]['time'] += servers_task_time[best_server]
    CLIENTS[client_address] = SERVERS[best_server]['time']

    LOCK.release()
    return best_server

def connectToServer(server):
    should_close_socket = False
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        should_close_socket = True
        server_address = (server[1]['address'], LISTENING_PORT)
        sock.connect(server_address)
    except socket.error as err:
        LBLogger(err)
        LBLogger('Failed to open socket')
        if should_close_socket:
            sock.close()
        sys.exit(1)
    server[1]['socket'] = sock

def connectToAllServers():
    LBLogger('Connecting to servers')
    for server in SERVERS.iteritems():
        connectToServer(server)

def startLBServer():
    should_close_socket = False
    try:
        lb_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        should_close_socket = True
        lb_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lb_server.bind((LB_SERVER_ADDRESS, LISTENING_PORT))
        LBLogger("Server started")
        LBLogger("Waiting for client request..")
        while True:
            lb_server.listen(5)
            client_socket, client_address = lb_server.accept()
            client_request_thread = ClientThread(client_address, client_socket)
            client_request_thread.start()
    except socket.error as err:
        LBLogger(err)
        LBLogger('Failed to open socket')
        if should_close_socket:
            lb_server.close()
        sys.exit(1)

def main():
    LBLogger('LB Started')
    try:
        connectToAllServers()
        startLBServer()
    except:
        LBLogger('Got unexcepted error')
        LBLogger(sys.exc_info()[0])

if __name__ == '__main__':
    main()

