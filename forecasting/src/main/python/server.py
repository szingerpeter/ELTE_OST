import socket
import threading

connections = []
total_connections = 0

HOST_NAME = "localhost"
PORT = 8080

class Client(threading.Thread):
    def __init__(self, socket, address, id, signal):
        threading.Thread.__init__(self)
        self.socket = socket
        self.address = address
        self.id = id
        self.signal = signal
    
    def __str__(self):
        return str(self.id) + " " + str(self.address)
    
    def run(self):
        while self.signal:
            try:
                data = self.socket.recv(1024)
            except:
                print("Client " + str(self.id) + " has disconnected")
                self.signal = False
                connections.remove(self)
                break
            if data != "":
                print("Client " + str(self.id) + ": " + str(data.decode("utf-8")))

                # CALL TS MODULE HERE WITH NEW DATA

                for client in connections:
                    if client.id != self.id:
                        client.socket.sendall(data)

def newConnections(socket):
    while True:
        sock, address = socket.accept()
        global total_connections
        connections.append(Client(sock, address, total_connections, True))
        connections[-1].start()
        print("New connection at ID " + str(connections[-1]))
        total_connections += 1

def main():
    host = HOST_NAME
    port = PORT

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen(5)

    newConnectionsThread = threading.Thread(target = newConnections, args = (sock,))
    newConnectionsThread.start()
    
main()