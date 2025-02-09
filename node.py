import socket
import threading
from utils import hash_key

class Node:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.node_id = hash_key(f"{ip}:{port}")
        self.successor = self
        self.predecessor = self
        self.data = {}  # Stores <key, value> pairs locally

    def start_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.ip, self.port))
        server.listen(5)
        print(f"Node {self.node_id} listening on {self.ip}:{self.port}")

        while True:
            client, _ = server.accept()
            threading.Thread(target=self.handle_request, args=(client,)).start()

    def handle_request(self, client):
        request = client.recv(1024).decode().strip()
        parts = request.split()
        command = parts[0].lower()

        if command == "insert" and len(parts) == 3:
            key, value = parts[1], parts[2]
            self.insert(key, value)
            response = f"Inserted key {key} with value {value}"
        elif command == "query" and len(parts) == 2:
            key = parts[1]
            response = f"Query result: {self.query(key)}"
        elif command == "delete" and len(parts) == 2:
            key = parts[1]
            response = f"Deleted key {key}" if self.delete(key) else "Key not found"
        elif command == "join":
            # Handling join request
            joining_ip, joining_port = parts[1], int(parts[2])
            response = self.handle_join_request(joining_ip, joining_port)
        else:
            response = "Invalid command"

        client.send(response.encode())
        client.close()

    def insert(self, key, value):
        hashed_key = hash_key(key)
        if self.responsible_for(hashed_key):
            if key in self.data:
                self.data[key] += f", {value}"  # Concatenate for update
            else:
                self.data[key] = value
        else:
            self.forward_request("insert", key, value)

    def query(self, key):
        hashed_key = hash_key(key)
        if self.responsible_for(hashed_key):
            return self.data.get(key, "Key not found")
        else:
            return self.forward_request("query", key)

    def delete(self, key):
        hashed_key = hash_key(key)
        if self.responsible_for(hashed_key):
            return self.data.pop(key, None)
        else:
            return self.forward_request("delete", key)

    def forward_request(self, command, key, value=None):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((self.successor.ip, self.successor.port))
            message = f"{command} {key}" if value is None else f"{command} {key} {value}"
            client.sendall(message.encode())
            response = client.recv(1024).decode()
            return response

    def responsible_for(self, hashed_key):
        if self.predecessor is None:
            return True
        return self.predecessor.node_id < hashed_key <= self.node_id

    def join(self, bootstrap_ip, bootstrap_port):
        print(f"Node {self.node_id} joining via {bootstrap_ip}:{bootstrap_port}")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((bootstrap_ip, bootstrap_port))
            client.sendall(f"join {self.ip} {self.port}".encode())
            response = client.recv(1024).decode()
            print(response)

    def handle_join_request(self, joining_ip, joining_port):
        # Simple logic to update successor/predecessor on join
        print(f"Handling join request from {joining_ip}:{joining_port}")
        # Further logic to set successor/predecessor pointers and redistribute keys would go here
        return "Join request processed"

    def depart(self):
        print(f"Node {self.node_id} departing")
        # Further logic to redistribute keys and update pointers would go here
