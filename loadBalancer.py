import socket, SocketServer, Queue, sys, time, threading
http_port = 80
lock = threading.Lock()
SERV_HOST = '10.0.0.1'
CLIENTS = {}
servers = {'serv1' : {'M':2, 'P':1, 'V':1, 'time':0, 'address': '192.168.0.101', 'socket' : None},
           'serv2' : {'M':2, 'P':1, 'V':1, 'time':0, 'address': '192.168.0.102', 'socket' : None},
           'serv3' : {'M':1, 'P':2, 'V':3, 'time':0, 'address': '192.168.0.103', 'socket' : None}}

def LBPrint(string):
    print '%s: %s-----' % (time.strftime('%H:%M:%S', time.localtime(time.time())), string)

def getServerSocket(name):
    return servers[name]['socket']


def getServerAddr(name):
    return servers[name]['address']


def update_servers_time(current_time):
    servers['serv1']['time'] = max(current_time, servers['serv1']['time'])
    servers['serv2']['time'] = max(current_time, servers['serv2']['time'])
    servers['serv3']['time'] = max(current_time, servers['serv3']['time'])


def getNextServer(client_address, req_type, req_len):
    global lock
    lock.acquire()
    req_len = int(req_len)
    if client_address in CLIENTS:
        current_time = CLIENTS[client_address]
    else: # First request of a client is in TIME=0
        current_time = 0

    update_servers_time(current_time)

    servers_task_time = {}
    servers_task_time['serv1'] = servers['serv1'][req_type] * req_len
    servers_task_time['serv2'] = servers['serv2'][req_type] * req_len
    servers_task_time['serv3'] = servers['serv3'][req_type] * req_len

    serv1_task_end_time = servers_task_time['serv1'] + servers['serv1']['time']
    serv2_task_end_time = servers_task_time['serv2'] + servers['serv2']['time']
    serv3_task_end_time = servers_task_time['serv3'] + servers['serv3']['time']


    if serv1_task_end_time < serv2_task_end_time:
        if serv1_task_end_time < serv3_task_end_time:
            next_server = 'serv1'
        else:
            next_server = 'serv3'
    elif serv2_task_end_time < serv3_task_end_time:
        next_server = 'serv2'
    else:
        next_server = 'serv3'

    servers[next_server]['time'] += servers_task_time[next_server]
    CLIENTS[client_address] = servers[next_server]['time']

    lock.release()
    return next_server


def parseRequest(req):
    return (
     req[0], req[1])


class LoadBalancerRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        client_sock = self.request
        req = client_sock.recv(2)
        req_type, req_len = parseRequest(req)
        server_name = getNextServer(self.client_address, req_type, req_len)
        LBPrint('recieved request %s from %s, sending to %s' % (req, self.client_address[0], getServerAddr(server_name)))
        serv_sock = getServerSocket(server_name)
        serv_sock.sendall(req)
        data = serv_sock.recv(2)
        client_sock.sendall(data)
        client_sock.close()


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


def connectToServer(server):
    should_close_socket = False
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        should_close_socket = True
        server_address = (server[1]['address'], http_port)
        sock.connect(server_address)
    except socket.error as err:
        LBPrint(err)
        LBPrint('Failed to open socket')
        if should_close_socket:
            sock.close()
        sys.exit(1)
    server[1]['socket'] = sock


def connectToAllServers():
    LBPrint('Connecting to servers')
    for server in servers.iteritems():
        connectToServer(server)


def main():
    LBPrint('LB Started')
    try:
        connectToAllServers()
        server = ThreadedTCPServer((SERV_HOST, http_port), LoadBalancerRequestHandler)
        server.serve_forever()
    except socket.error as msg:
        LBPrint(msg)

if __name__ == '__main__':
    main()