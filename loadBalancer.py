import socket, SocketServer, Queue, sys, time, threading
HTTP_PORT = 80
lock = threading.Lock()
SERV_HOST = '10.0.0.1'
servers = {'serv1': ('192.168.0.101', None), 'serv2': ('192.168.0.102', None), 'serv3': ('192.168.0.103', None)}
CLIENTS = {}
servers_handle_times = {'serv1' : {'M':2, 'P':1, 'V':1, 'time':0},
                        'serv2' : {'M':2, 'P':1, 'V':1, 'time':0},
                        'serv3' : {'M':1, 'P':2, 'V':3, 'time':0}}

def LBPrint(string):
    print '%s: %s-----' % (time.strftime('%H:%M:%S', time.localtime(time.time())), string)


def createSocket(addr, port):
    for res in socket.getaddrinfo(addr, port, socket.AF_UNSPEC, socket.SOCK_STREAM):
        af, socktype, proto, canonname, sa = res
        try:
            new_sock = socket.socket(af, socktype, proto)
        except socket.error as msg:
            LBPrint(msg)
            new_sock = None
            continue
        else:
            try:
                new_sock.connect(sa)
            except socket.error as msg:
                LBPrint(msg)
                new_sock.close()
                new_sock = None
                continue
            else:
                break

    if new_sock is None:
        LBPrint('could not open socket')
        sys.exit(1)
    return new_sock


def getServerSocket(server_name):
    return servers[name][1]


def getServerAddr(servID):
    name = 'serv%d' % servID
    return servers[name][0]


def update_servers_time(current_time):
    servers_handle_times['serv1']['time'] = max(current_time, servers_handle_times['serv1']['time'])
    servers_handle_times['serv2']['time'] = max(current_time, servers_handle_times['serv2']['time'])
    servers_handle_times['serv3']['time'] = max(current_time, servers_handle_times['serv3']['time'])


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
    servers_task_time['serv1'] = servers_handle_times['serv1'][req_type] * req_len
    servers_task_time['serv2'] = servers_handle_times['serv2'][req_type] * req_len
    servers_task_time['serv3'] = servers_handle_times['serv3'][req_type] * req_len

    serv1_task_end_time = servers_task_time['serv1'] + servers_handle_times['serv1']['time']
    serv2_task_end_time = servers_task_time['serv2'] + servers_handle_times['serv2']['time']
    serv3_task_end_time = servers_task_time['serv3'] + servers_handle_times['serv3']['time']


    if serv1_task_end_time < serv2_task_end_time:
        if serv1_task_end_time < serv3_task_end_time:
            next_server = 'serv1'
        else:
            next_server = 'serv3'
    elif serv2_task_end_time < serv3_task_end_time:
        next_server = 'serv2'
    else:
        next_server = 'serv3'

    servers_handle_times[next_server]['time'] += servers_task_time[next_server]
    CLIENTS[client_address] = servers_handle_times[next_server]['time']

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
        LBPrint('recieved request %s from %s, sending to %s' % (req, self.client_address[0], getServerAddr(servID)))
        serv_sock = getServerSocket(server_name)
        serv_sock.sendall(req)
        data = serv_sock.recv(2)
        client_sock.sendall(data)
        client_sock.close()


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


if __name__ == '__main__':
    print "Moran"
    try:
        LBPrint('LB Started')
        LBPrint('Connecting to servers')
        for name, (addr, sock) in servers.iteritems():
            servers[name] = (
             addr, createSocket(addr, HTTP_PORT))

        server = ThreadedTCPServer((SERV_HOST, HTTP_PORT), LoadBalancerRequestHandler)
        server.serve_forever()
    except socket.error as msg:
        LBPrint(msg)