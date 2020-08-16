import socket

class KafkaConnection(object):
    def __init__(self, host, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect()
    
    def send(self, data):
        pass

    def recv(self):
        pass

    def close(self):
        self.sock.close()
