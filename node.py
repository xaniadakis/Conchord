import socket
import threading
import time
import json

from utils import hash_key, log, custom_split

class Node:
    def __init__(self, ip, port, bootstrap = False, replication_factor=3, consistency="chain"):
        self.ip = ip
        self.port = port
        self.node_id = hash_key(f"{ip}:{port}")
        self.bootstrap_node = bootstrap
        self.successor = self
        self.predecessor = self
        self.data = {}

        if not self.bootstrap_node:
            self.prefix = f"[NODE {str(self.node_id)[-4:]}]: "
        else:
            self.prefix = F"[BOOTSTRAP NODE | {str(self.node_id)[-4:]}]: "
        self.replication_factor = replication_factor  # k replicas
        self.consistency = consistency

    def log(self, output=None):
        log(prefix=self.prefix, output=output)

    def start_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.server_socket.bind((self.ip, self.port))
        self.server_socket.listen(5)
        self.log(f"Listening on {self.ip}:{self.port}")

        while True:
            client, _ = self.server_socket.accept()
            threading.Thread(target=self.handle_request, args=(client,)).start()

    def handle_request(self, client):
        try:

            request = client.recv(1024).decode().strip()
            parts = custom_split(request)
            command = parts[0].lower()

            if command == "find_successor":
                node_id = int(parts[1])
                successor = self.find_successor(node_id)
                response = f"{successor.ip}:{successor.port}"
            elif command == "get_predecessor":
                response = f"{self.predecessor.ip}:{self.predecessor.port}" if self.predecessor and self.predecessor != self else "None"
            elif command == "update_predecessor":
                if len(parts) >= 3:
                    self.log(f"Updating predecessor {parts}")
                    try:
                        pred_ip = parts[1]
                        pred_port = int(parts[2])
                        self.predecessor = Node(pred_ip, pred_port)
                        self.log(f"Predecessor updated to {self.predecessor.node_id}")
                        response = "Predecessor updated"
                    except ValueError:
                        self.log(f"ValueError while updating predecessor: {parts}")
                        response = "ERROR: Invalid predecessor format"
                else:
                    response = "ERROR: Malformed update_predecessor command"
            elif command == "update_successor":
                if len(parts) >= 3:
                    self.log(f"Updating successor {parts}")
                    try:
                        succ_ip = parts[1]
                        succ_port = int(parts[2])
                        self.successor = Node(succ_ip, succ_port)
                        self.log(f"Successor updated to {self.successor.node_id}")
                        response = "Successor updated"
                    except ValueError:
                        self.log(f"ValueError while updating successor: {parts}")
                        response = "ERROR: Invalid successor format"
                else:
                    response = "ERROR: Malformed update_successor command"
            elif command == "transfer_keys":
                transfer_data = {key: value for key, value in self.data.items() if hash_key(key) <= self.node_id}
                self.data = {k: v for k, v in self.data.items() if k not in transfer_data}  # Remove transferred keys
                response = str(transfer_data)
            elif command == "receive_keys":
                keys_data = eval(" ".join(parts[1:]))  # Convert back to dictionary
                self.data.update(keys_data)
                response = "Keys received"

            elif command == "insert":
                key, value = parts[1], parts[2]
                if len(parts) == 4:
                    replica_count = int(parts[3])
                else:
                    replica_count = 0
                if replica_count >= self.replication_factor:
                    response = "Replication limit reached"
                else:
                    response = self.insert(key, value, replica_count) or "ERROR: Insert failed"
            elif command == "query":
                key = parts[1]
                initial_node, hops = None, 0
                if len(parts) == 4:
                    initial_node = parts[3]
                elif len(parts) == 3:
                    hops = int(parts[2])
                response = f"{self.query(key, hops=hops, initial_node=initial_node)}"
            elif command == "delete":
                key = parts[1]
                if len(parts) == 3:
                    replica_count = int(parts[2])
                else:
                    replica_count = 0
                if replica_count >= self.replication_factor:
                    response = "Replication limit reached"
                else:
                    response = self.delete(key, replica_count)
            elif command == "join":
                # Handling join request
                joining_ip, joining_port = parts[1], int(parts[2])
                response = self.handle_join_request(joining_ip, joining_port)
            else:
                response = f"Invalid command: {", ".join(parts)}"

            client.send(response.encode())
        except Exception as e:
            self.log(f"ERROR: Exception in handle_request: {e}")
            client.send("ERROR: Internal server error".encode())
        finally:
            client.close()

    def insert(self, key, value, replica_count=0):
        hashed_key = hash_key(key)

        if self.responsible_for(hashed_key) or replica_count>0:
            #print(f"Node {self.node_id} is responsible for key {hashed_key}")
            if replica_count==0:
                self.log(f"Responsible for key {key}")
            if key in self.data.keys():
                # no duplicates
                existing_values = self.data[key].split(", ")
                # concatenate for update
                if value not in existing_values:
                    self.data[key] += f", {value}"
            else:
                self.data[key] = value
            if replica_count < self.replication_factor - 1:
                if self.consistency == "chain":
                    return self.chain_replicate("insert", key, value, replica_count)
                elif self.consistency == "eventual":
                    threading.Thread(target=self.eventual_replicate, args=("insert", key, value, replica_count)).start()
                    return f"{self.prefix} Inserted {key}: {value}"
            elif replica_count == self.replication_factor - 1:
                self.log(f"Tail received baton for key {key}")
                if self.consistency == "chain":
                    return f"{self.prefix}Inserted {key}: {value}"
        else:
            # self.log(f"Forwarding key {key} to successor {str(self.successor.node_id)[-4:]}")
            return self.forward_request("insert", key, value)

    def query(self, key, hops=0, initial_node=None):
        """Handles queries based on consistency model."""
        hashed_key = hash_key(key)

        if key == "*":
            # the bootstrap node
            if initial_node is None:
                self.log(f"First query * call")
                initial_node = self.node_id

            # the last node before bootstrap returns its data
            if self.successor.node_id == int(initial_node):
                self.log(f"Last query * call, added {len(self.data)}")
                response = json.dumps(self.data, indent=4)
                return response

            # forward request to the successor, waiting for their response
            response = self.forward_request("query", key, initial_node=initial_node)

            # parse response safely
            if not response.strip():
                self.log("ERROR: Empty response received!")
                return json.dumps(self.data, indent=4)

            # convert response to dict
            try:
                received_data = json.loads(response)
            except json.JSONDecodeError as e:
                self.log(f"ERROR: JSON decode failed: {e}, response: {response}")
                # if failed return my data
                return json.dumps(self.data, indent=4)

            before = len(received_data)

            # extend dict with current node's data
            received_data.update(self.data)

            after = len(received_data)
            self.log(f"Added: {after - before} pairs, now hold: {after}")
            return json.dumps(received_data, indent=4)


        if self.consistency == "chain":
            if self.responsible_for(hashed_key) or hops > 0:
                if hops < self.replication_factor - 1:
                    return self.forward_request("query", key, hops=hops + 1)
                self.log(f"found {key}")
                return self.data.get(key, "Key not found")
            else:
                return self.forward_request("query", key)
        elif self.consistency == "eventual":
            if key in self.data.keys():
                self.log(f"found {key}")
                return self.data[key]
            return self.forward_request("query", key, hops=hops + 1)
        return "Key not found"

    def delete(self, key, replica_count=0):
        hashed_key = hash_key(key)
        if self.responsible_for(hashed_key) or replica_count > 0:
            self.log(f"Deleting key {key} {f'(replica {replica_count})' if replica_count>0 else ''}")
            self.data.pop(key, None)

            if replica_count < self.replication_factor - 1:
                if self.consistency == "chain":
                    return self.chain_replicate("delete", key, None, replica_count)
                elif self.consistency == "eventual":
                    threading.Thread(target=self.eventual_replicate, args=("delete", key, None, replica_count)).start()
                    return f"{self.prefix} Deleted {key}"
            elif replica_count == self.replication_factor - 1:
                self.log(f"Tail received baton to delete key {key}")
                if self.consistency == "chain":
                    return f"{self.prefix}Deleted {key}"
        else:
            return self.forward_request("delete", key)

    def chain_replicate(self, command, key, value, replica_count):
        """Passes replication baton strictly to the next successor in chain."""
        # if replica_count >= self.replication_factor - 1:
        #     return

        successor = self.successor
        if successor:
            self.log(f"Passing replication baton from {self.ip}:{self.port} to {successor.ip}:{successor.port} for key {key} and rc: {replica_count + 1}")
            return successor.forward_request(command, key, value, replica_count=replica_count + 1)

    def eventual_replicate(self, command, key, value, replica_count):
        """Lazy baton-passing replication (eventual consistency)."""
        # if replica_count >= self.replication_factor - 1:
        #     return

        successor = self.successor
        if successor:
            time.sleep(0.1)  # Simulate async delay
            self.log(f"Lazy forwarding to {successor.ip}:{successor.port} for key {key}")
            successor.forward_request(command, key, value, replica_count=replica_count + 1)

    def forward_request(self, command, key, value=None, replica_count=0, hops=0, initial_node=None):
        """Forwards request to successor, passing the baton along."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.settimeout(2)  # Prevent infinite waiting
            client.connect((self.successor.ip, self.successor.port))
            message = f"{command} {key}" if value is None else f"{command} {key} {value}"
            if replica_count > 0:
                message += f" {replica_count}"
            if hops > 0:
                message += f" {hops}"
            if initial_node is not None:
                message += f" 0 {initial_node}"

            client.sendall(message.encode())
            if command != "query":
                response = client.recv(1024).decode()
            else:
                # a solution to receive huge responses dynamically (like the query response)
                response = []
                while True:
                    try:
                        chunk = client.recv(1024).decode()  # read chunks
                        if not chunk:
                            break  # stop when no more data arrives
                        response.append(chunk)
                    except socket.timeout:
                        break  # stop if no data arrives within timeout

                response = "".join(response)
            return response

    def responsible_for(self, key_hash):
        """Check if this node is responsible for a given hashed key."""
        pred_id = self.predecessor.node_id if self.predecessor else None
        node_id = self.node_id

        # Single node case
        if pred_id is None or pred_id == node_id:
            return True

        # Normal case: predecessor < node_id
        if pred_id < node_id:
            return pred_id < key_hash <= node_id

        # Wrap-around case: predecessor > node_id (means we're at the 0-boundary)
        return key_hash > pred_id or key_hash <= node_id

    def handle_join_request(self, joining_ip, joining_port):
        # Simple logic to update successor/predecessor on join
        self.log(f"Handling join request from {joining_ip}:{joining_port}")
        # Further logic to set successor/predecessor pointers and redistribute keys would go here
        return "Join request processed"

    def depart(self):
        self.log(f"Departing...")

        # Step 1: Transfer keys to successor before leaving
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect((self.successor.ip, self.successor.port))
                client.sendall(f"receive_keys {str(self.data)}".encode())
                ack = client.recv(1024).decode()
                if ack != "ACK":
                    self.log("ERROR: Successor did not confirm key transfer!")
        except:
            self.log(f"ERROR: Could not transfer keys to {self.successor.ip}:{self.successor.port}")

        # Step 2: Notify predecessor to update successor
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect((self.predecessor.ip, self.predecessor.port))
                client.sendall(f"update_successor {self.successor.ip} {self.successor.port}".encode())
                ack = client.recv(1024).decode()
                if ack != "ACK":
                    self.log("ERROR: Predecessor did not acknowledge successor update!")
        except:
            self.log(f"ERROR: Could not update predecessor at {self.predecessor.ip}:{self.predecessor.port}")

        # Step 3: Notify successor to update predecessor
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect((self.successor.ip, self.successor.port))
                client.sendall(f"update_predecessor {self.predecessor.ip} {self.predecessor.port}".encode())
                ack = client.recv(1024).decode()
                if ack != "ACK":
                    self.log("ERROR: Successor did not acknowledge predecessor update!")
        except:
            self.log(f"ERROR: Could not update successor at {self.successor.ip}:{self.successor.port}")

        self.log("Closing socket...")
        self.server_socket.close()
        self.log("Successfully left the Chord ring.")

    def join(self, bootstrap_ip, bootstrap_port):
        self.log(f"Joining via {bootstrap_ip}:{bootstrap_port}")
        
        # Step 1: Find the correct successor based on my ID
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((bootstrap_ip, bootstrap_port))
            client.sendall(f"find_successor {self.node_id}".encode())  # Ask bootstrap to find successor
            successor_data = client.recv(1024).decode()

        succ_ip, succ_port = successor_data.split(":")
        self.successor = Node(succ_ip, int(succ_port))

        # Step 2: Get my correct predecessor (not always bootstrap!)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((self.successor.ip, self.successor.port))
            client.sendall("get_predecessor".encode())  # Ask successor for its current predecessor
            pred_data = client.recv(1024).decode()

        if pred_data == "None":  # Bootstrap node is alone
            self.predecessor = self.successor

            # Bootstrap should update its successor!
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect((bootstrap_ip, bootstrap_port))
                client.sendall(f"update_successor {self.ip} {self.port}".encode())
            
        else:
            pred_ip, pred_port = pred_data.split(":")
            self.predecessor = Node(pred_ip, int(pred_port))

            # Step 3: Notify old predecessor to update its successor to me
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect((self.predecessor.ip, self.predecessor.port))
                client.sendall(f"update_successor {self.ip} {self.port}".encode())

        # Step 4: Notify my successor to update its predecessor to me
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((self.successor.ip, self.successor.port))
            client.sendall(f"update_predecessor {self.ip} {self.port}".encode())
            self.log(f"Successfully joined the Chord ring.")

    def find_successor(self, node_id):
        # Single node in the ring case
        if self.node_id == self.successor.node_id:
            return self

        # If the node ID fits between this node and its successor, return the successor
        if self.node_id < node_id <= self.successor.node_id:
            return self.successor

        # Handle wrap-around case when IDs roll over the hash range
        if self.node_id > self.successor.node_id:
            if node_id > self.node_id or node_id <= self.successor.node_id:
                return self.successor          

        # Forward request to next node
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect((self.successor.ip, self.successor.port))
                client.sendall(f"find_successor {node_id}".encode())
                response = client.recv(1024).decode()
                successor_ip, successor_port = response.split(":")
                return Node(successor_ip, int(successor_port))
        except:
            self.log(f"ERROR: Forwarding find_successor failed to {self.successor.ip}:{self.successor.port}")
            return self  # Safe fallback
