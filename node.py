import socket
import threading
import time
import json
import argparse

from click import command
from colorama import Fore, Style, init
import signal
import os
import copy

init(autoreset=True)

from utils import hash_key, log, custom_split

class Node:
    def __init__(self, ip, port, bootstrap_ip=None, bootstrap_port=None,
                 bootstrap = False, replication_factor=3, consistency="chain"):
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

        if self.bootstrap_node:
            self.replication_factor = replication_factor
            self.consistency = consistency

        if not bootstrap and bootstrap_ip and bootstrap_port:
            self.join(bootstrap_ip, bootstrap_port)

    def log(self, output=None):
        log(prefix=self.prefix, output=output)

    def start_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        bind_ip = "0.0.0.0"
        self.server_socket.bind((bind_ip, self.port))
        self.server_socket.listen(5)
        self.log(f"Binding on {bind_ip}:{self.port}")

        while True:
            client, _ = self.server_socket.accept()
            threading.Thread(target=self.handle_request, args=(client,)).start()

    def join(self, bootstrap_ip, bootstrap_port):
        self.log(f"Joining via Bootstrap Node => {bootstrap_ip}:{bootstrap_port}")

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect((bootstrap_ip, bootstrap_port))
                client.sendall("get_network_config".encode())
                config_data = client.recv(1024).decode()
                replication_factor, consistency = config_data.split(":")
                self.replication_factor = int(replication_factor)
                self.consistency = consistency
                self.log(f"Received network config: Replication Factor={self.replication_factor}, "
                         f"Consistency={self.consistency}")

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect((bootstrap_ip, bootstrap_port))
                client.sendall(f"find_successor {self.node_id}".encode())
                successor_data = client.recv(1024).decode()

            succ_ip, succ_port = successor_data.split(":")
            self.successor = Node(succ_ip, int(succ_port))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect((self.successor.ip, self.successor.port))
                client.sendall("get_predecessor".encode())
                pred_data = client.recv(1024).decode()

            if pred_data == "None":
                self.predecessor = self.successor
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                    client.connect((bootstrap_ip, bootstrap_port))
                    client.sendall(f"update_successor {self.ip} {self.port}".encode())
            else:
                pred_ip, pred_port = pred_data.split(":")
                self.predecessor = Node(pred_ip, int(pred_port))
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                    client.connect((self.predecessor.ip, self.predecessor.port))
                    client.sendall(f"update_successor {self.ip} {self.port}".encode())

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect((self.successor.ip, self.successor.port))
                client.sendall(f"update_predecessor {self.ip} {self.port}".encode())
                self.log(f"Successfully joined the Chord ring.")

            # Request keys that now belong to this new node
            # The successor transfers to the new node - predecessor - the primary data it is responsible for,
            # along with all its replicas. The successor then increments the hop count for all transferred data
            # and propagates this update to the next successors.  If the hop count exceeds the replication factor,
            # the corresponding keys are deleted.
            self.log(f"Requesting keys from successor {self.successor.ip}:{self.successor.port}")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect((self.successor.ip, self.successor.port))
                client.sendall(f"transfer_keys {self.node_id}".encode())
                client.settimeout(2)
                received_data = []
                try:
                    while True:
                        chunk = client.recv(4096).decode()
                        if not chunk:
                            break
                        received_data.append(chunk)
                        if len(chunk) < 4096:
                            break
                except socket.timeout:
                    self.log("[ERROR] Receiving data timed out after 2 seconds.")

                self.log(f"I received {len(received_data)} bytes for transfer_keys.")

            # if data received, store in this node's data and acknowledge
            if received_data:
                try:
                    transferred_keys = json.loads("".join(received_data))
                    self.data.update(transferred_keys)
                    self.log(f"Received {len(transferred_keys)} keys from successor.")

                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                        client.connect((self.successor.ip, self.successor.port))
                        client.sendall("ACK".encode())
                except json.JSONDecodeError as e:
                    self.log(f"{Fore.RED}ERROR: Failed to parse received key data: {e}{Style.RESET_ALL}")
            else:
                self.log(f"Did not receive transfer_keys")

        except Exception as e:
            self.log(f"{Fore.RED}ERROR: Join failed - {e}{Style.RESET_ALL}")

    def get_overlay(self, initial_node=None):
        if initial_node is None:
            self.log("Initiating overlay collection")
            initial_node = self.node_id

        key_count = len(self.data) if hasattr(self, "data") else 0
        overlay_data = {
            "node_id": self.node_id,
            "ip": self.ip,
            "port": self.port,
            "successor": self.successor.node_id if self.successor else None,
            "predecessor": self.predecessor.node_id if self.predecessor else None,
            "is_bootstrap": self.bootstrap_node,
            "key_count": key_count
        }

        # if request returns to the initial node, return full collected data
        if self.successor.node_id == int(initial_node):
            self.log(f"Final overlay call, returning data.")
            return json.dumps({self.node_id: overlay_data}, indent=4)

        # forward request to successor and collect response
        response = self.forward_request("overlay", initial_node=initial_node)

        # if response is empty, return only this node's data
        if not response.strip():
            self.log(f"{Fore.RED}ERROR: Empty overlay response received!{Style.RESET_ALL}")
            return json.dumps({self.node_id: overlay_data}, indent=4)

        # convert response to dictionary
        try:
            received_overlay = json.loads(response)
        except json.JSONDecodeError as e:
            self.log(f"{Fore.RED}ERROR: JSON decode failed: {e}, response: {response}{Style.RESET_ALL}")
            return json.dumps({self.node_id: overlay_data}, indent=4)  # Return only self-data if parsing fails

        # merge received data with current node
        received_overlay[self.node_id] = overlay_data

        self.log(f"Overlay aggregation: now holding {len(received_overlay)} nodes")
        return json.dumps(received_overlay, indent=4)

    def reset_configuration(self, replication_factor, consistency, initial_node=None):
        if initial_node is None:
            self.log("Initiating reset configuration process.")
            initial_node = self.node_id

        self.replication_factor = int(replication_factor)
        self.consistency = consistency
        self.data.clear()
        self.log(
            f"Reset configuration: Replication Factor={self.replication_factor}, Consistency={self.consistency}, Data Cleared.")

        # if this node's successor is the initial node, stop propagation
        if self.successor.node_id == int(initial_node):
            self.log("Final reset_config call, stopping propagation.")
            return json.dumps({str(self.node_id)[-4:]: "ACK"}, indent=4)

        # forward request to successor and collect response
        response = self.forward_request(command="reset_config", replication_factor=str(replication_factor),
                                        consistency=consistency, initial_node=initial_node)

        # if response is empty, return only this node's data
        if not response.strip():
            self.log(f"{Fore.RED}ERROR: Empty reset response received!{Style.RESET_ALL}")
            return json.dumps({str(self.node_id)[-4:]: "ACK"}, indent=4)

        # convert response to dictionary
        try:
            received_reset_status = json.loads(response)
        except json.JSONDecodeError as e:
            self.log(f"{Fore.RED}ERROR: JSON decode failed: {e}, response: {response}{Style.RESET_ALL}")
            return json.dumps({str(self.node_id)[-4:]: "ACK"}, indent=4)  # Return only self-data if parsing fails

        # merge received data with current node
        received_reset_status[str(self.node_id)[-4:]] = "ACK"

        self.log(f"Reset aggregation: now holding reset status for {len(received_reset_status)} nodes")
        return json.dumps(received_reset_status, indent=4)

    def handle_request(self, client):
        try:
            client.settimeout(2)
            buffer = []
            try:
                while True:
                    chunk = client.recv(1024*10).decode()
                    if not chunk:
                        break
                    buffer.append(chunk)
                    if len(chunk) < 1024*10:
                        break
            except socket.timeout:
                print("[ERROR] Receiving data timed out after 2 seconds.")
            request = "".join(buffer).strip()

            parts = custom_split(request)
            command = parts[0].lower()

            if command == "reset_config":
                new_replication_factor = parts[1]
                new_consistency = parts[2]
                if len(parts) == 3:
                    initial_node = None
                elif len(parts) == 4:
                    initial_node = parts[3]
                else:
                    response = json.dumps({"error": "Invalid reset_config command format"})
                self.log(
                    f"Resetting network config to Replication Factor={new_replication_factor}, Consistency={new_consistency}")
                response = self.reset_configuration(new_replication_factor, new_consistency, initial_node)
            elif command == "get_network_config":
                self.log("Sending network config.")
                response = f"{self.replication_factor}:{self.consistency}"
            elif command == "overlay":
                initial_node = None
                if len(parts) == 2:
                    initial_node = parts[1]
                response = self.get_overlay(initial_node=initial_node)
            elif command == "get_data":
                if len(parts) >= 2:
                    request_node_id = parts[1]
                    self.log(f"get_data {request_node_id}")
                    if str(self.node_id)[-4:] == request_node_id or str(self.node_id) == request_node_id:
                        response = json.dumps({"node_id": str(self.node_id), "data": self.data}, indent=4)
                        self.log(f"returning data {str(self.node_id)[-4:]}")
                    else:
                        self.log(f"forwarding data to {self.successor.node_id}")
                        response = self.forward_request(command="get_data", key=request_node_id)
                        self.log(f"received response from {str(self.successor.node_id)[-4:]}")
            elif command == "find_successor":
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
                        self.log(f"Predecessor updated to {str(self.predecessor.node_id)[-4:]}")
                        response = "ACK"
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
                        self.log(f"Successor updated to {str(self.successor.node_id)[-4:]}")
                        response = "ACK"
                    except ValueError:
                        self.log(f"ValueError while updating successor: {parts}")
                        response = "ERROR: Invalid successor format"
                else:
                    response = "ERROR: Malformed update_successor command"
            elif command == "transfer_keys":
                if len(parts) == 2:
                    new_node_id = int(parts[1])
                    self.log(f"Transferring primary keys to new predecessor {str(new_node_id)[-4:]}.")

                    # find primary keys (hop == 0) that should be transferred to the new node
                    primary_transfer_data = {key: value for key, value in self.data.items()
                                     if value["hop"] == 0 and hash_key(key) <= new_node_id}
                    self.log(f"Found {len(primary_transfer_data)} primary keys to transfer.")

                    replica_transfer_data = {key: value for key, value in self.data.items()
                                     if value["hop"] > 0}
                    self.log(f"Found {len(replica_transfer_data)} replicas to transfer.")

                    combined_transfer_data = copy.deepcopy(primary_transfer_data)
                    combined_transfer_data.update(copy.deepcopy(replica_transfer_data))
                    self.log(f"Total keys to transfer (primary + replicas): {len(combined_transfer_data)}")

                    # delete keys where hop > replication_factor - 1
                    old_data_size = len(self.data)
                    self.data = {k: v for k, v in self.data.items()
                                 if not (k in combined_transfer_data and v["hop"] >= self.replication_factor - 1)}
                    new_data_size = len(self.data)
                    self.log(f"Deleted {old_data_size - new_data_size} keys (hop >= {self.replication_factor - 1})."
                             f"Now have {new_data_size} keys.")

                    # increment hop for all of the keys of the successor that we send to the predecessor
                    for key in combined_transfer_data:
                         if key in self.data:
                             self.data[key]["hop"] += 1

                    # notify successors to increment hop on those keys too,
                    # and delete the ones that surpass the replication factor.
                    combined_transfer_keys = set(combined_transfer_data.keys())
                    if len(combined_transfer_keys) > 0:
                        increment_hop_response = self.forward_request(command="increment_hop",
                                                                      combined_transfer_keys=json.dumps(list(combined_transfer_keys)))

                        # increment_hop_response = self.forward_request("increment_hop", list(combined_transfer_keys))
                        # self.log(f"received response from {str(self.successor.node_id)[-4:]} : {increment_hop_response}")

                    # send the keys to the requesting node
                    response = json.dumps(combined_transfer_data)
                else:
                    response = json.dumps({"error": "Invalid transfer_keys command format"})

            elif command == "increment_hop":
                if len(parts) == 2:
                    try:
                        # deserialize JSON argument to list
                        keys_to_increment_hop = json.loads(parts[1])
                        if not isinstance(keys_to_increment_hop, list):
                            response = "ERROR: Expected a list of keys"
                        else:
                            # increment hop on those keys
                            for key in keys_to_increment_hop:
                                if key in self.data.keys():
                                    self.data[key]["hop"] += 1
                            # delete keys where hop > replication_factor - 1
                            old_data_size = len(self.data)
                            self.data = {k: v for k, v in self.data.items() if not v["hop"] > self.replication_factor - 1}
                            new_data_size = len(self.data)
                            self.log(
                                f"Deleted {old_data_size - new_data_size} keys (hop > {self.replication_factor - 1}). "
                                f"Now have {new_data_size} keys.")
                            print(f"old_data_size: {old_data_size}")
                            print(f"new_data_size: {new_data_size}")
                            print(f"len(keys_to_increment_hop): {len(keys_to_increment_hop)}")
                            if not old_data_size == new_data_size and len(keys_to_increment_hop) > 0:
                                increment_hop_response = self.forward_request(command="increment_hop",
                                                                              key=json.dumps(list(keys_to_increment_hop)))
                                self.log(
                                    f"received response from {str(self.successor.node_id)[-4:]} : {increment_hop_response}")
                            response = "ACK"
                    except json.JSONDecodeError:
                        response = "ERROR: Malformed JSON in increment_hop"
                else:
                    response = "ERROR: Malformed increment_hop command"
            elif command == "receive_keys":
                try:
                    keys_data = json.loads(" ".join(parts[1:]))
                    keys_data = {key.strip().strip('"').strip(): value for key, value in keys_data.items()}
                    alter_count = 0
                    insert_count = 0
                    transfer_data = {}
                    def normalize_key(key):
                        return key.strip().strip('"').strip("'")

                    # match keys ignoring extra quotes
                    normalized_self_data = {normalize_key(k): k for k in self.data}
                    for key, value in keys_data.items():
                        norm_key = normalize_key(key)
                        if norm_key in normalized_self_data:
                            original_key = normalized_self_data[norm_key]
                            # if the key already exists, decrement the hop count
                            self.data[original_key]["hop"] -= 1
                            transfer_data[original_key] = self.data[original_key]
                            alter_count += 1
                        else:
                            # insert the key with the received hop count and value
                            self.data[key] = {"value": value["value"], "hop": value["hop"]}
                            insert_count += 1

                    self.log(f"Received {len(keys_data)}, inserted {insert_count} new keys & altered {alter_count} keys after node "
                             f"departure from {str(self.predecessor.node_id)[-4:]}.")

                    # propagate the key transfer process to the next successor
                    if len(transfer_data) > 0 and alter_count+insert_count>0:
                        if self.successor.node_id != self.node_id:
                            self.log(f"Will propagate {len(transfer_data)} changes to {str(self.successor.node_id)[-4:]}")
                            ack = self.forward_request(command="receive_keys", key=json.dumps(transfer_data))
                            if ack == "ACK":
                                self.log(f"Successor {str(self.successor.node_id)[-4:]} acknowledged key transfer.")
                                response = "ACK"
                            else:
                                self.log(f"[ERROR] Successor {str(self.successor.node_id)[-4:]} did not acknowledge key transfer: {ack}")
                                response = "ERROR"
                        else:
                            response = "ACK"
                    else:
                        response = "ACK"

                except json.JSONDecodeError:
                    response = "ERROR: Invalid key transfer data format"

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

                if len(parts) == 3:
                    if len(str(parts[2])) > 4:
                        initial_node = parts[2]
                    else:
                        hops = int(parts[2])
                elif len(parts) == 4:
                    if len(str(parts[2])) > 4:
                        initial_node = parts[2]
                        hops = int(parts[3])
                    else:
                        hops = int(parts[2])
                        initial_node = parts[3]
                value = self.query(key, hops=hops, initial_node=initial_node)
                response = f"{value}"
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

            else:
                response = f"Invalid command: {", ".join(parts)}"

            # ensure socket is valid before responding
            if client.fileno() != -1:
                client.send(response.encode())
            else:
                self.log(f"Socket is not open anymore. Client cannot send response!")
        except Exception as e:
            self.log(f"{Fore.RED}ERROR: Exception in handle_request: {e}{Style.RESET_ALL}")
            if client.fileno() != -1:
                try:
                    client.send("ERROR: Internal server error".encode())
                except OSError:
                    pass
        finally:
            try:
                client.close()
            except OSError:
                pass

    def insert(self, key, value, replica_count=0):
        hashed_key = hash_key(key)

        if self.responsible_for(hashed_key) or replica_count>0:
            #print(f"Node {self.node_id} is responsible for key {hashed_key}")
            if replica_count==0:
                self.log(f"Responsible for key {key}:{value}")
            if key in self.data:
                # no duplicates
                existing_values = self.data[key]["value"].split(", ")
                # concatenate for update
                if value not in existing_values:
                    self.data[key]["value"] += f", {value}"
            else:
                self.data[key] = {"value": value, "hop": replica_count}

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
            return self.forward_request(command="insert", key=key, value=value)

    def query(self, key, hops=0, initial_node=None):
        hashed_key = hash_key(key)
        if key == "*" or key.strip().strip('"').strip() == "*":
            self.log(f"Got query {key} {initial_node}")
            # the bootstrap node
            if initial_node is None:
                self.log(f"First query * call")
                initial_node = self.node_id

            # the last node before bootstrap returns its data
            if self.successor.node_id == int(initial_node):
                self.log(f"Last query * call, added {len(self.data)}")
                return json.dumps({k: v["value"] for k, v in self.data.items()}, indent=4)

            # forward request to the successor, waiting for their response
            self.log(f"Forwarding to {self.successor.node_id} query {key} {initial_node}")
            response = self.forward_request(command="query", key=key, hops=0, initial_node=initial_node)

            # parse response safely
            if not response.strip():
                self.log(f"{Fore.RED}ERROR: Empty response received!{Style.RESET_ALL}")
                return json.dumps({k: v["value"] for k, v in self.data.items()}, indent=4)

            # convert response to dict
            try:
                received_data = json.loads(response)
            except json.JSONDecodeError as e:
                self.log(f"{Fore.RED}ERROR: JSON decode failed: {e}, response: {response}{Style.RESET_ALL}")
                # if failed return my data
                return json.dumps({k: v["value"] for k, v in self.data.items()}, indent=4)

            before = len(received_data)
            # extend dict with current node's data
            received_data.update({k: v["value"] for k, v in self.data.items()})
            after = len(received_data)
            self.log(f"Added: {after - before} pairs, now hold: {after}")
            return json.dumps(received_data, indent=4)
        if self.consistency == "chain":
            if self.responsible_for(hashed_key) or hops > 0:
                if hops < self.replication_factor - 1:
                    return self.forward_request(command="query", key=key, hops=hops + 1)
                return self.data[key]["value"] if key in self.data else "Key not found"
            else:
                return self.forward_request(command="query", key=key)
        elif self.consistency == "eventual":
            if initial_node is None:
                self.log(f"First eventual query call: {key}")
                initial_node = self.node_id
            else:
                self.log(f"{hops}) eventual query call: {key}, init_node: {initial_node}")
            if key in self.data.keys():
                self.log(f"found {key}")
                return self.data[key]["value"]
            if self.successor.node_id == int(initial_node):
                self.log(f"{Fore.YELLOW}Last eventual query call. Key {key} not found.{Style.RESET_ALL}")
                return "Key not found"
            return self.forward_request(command="query", key=key, hops=hops+1, initial_node=initial_node)
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
            return self.forward_request(command="delete", key=key)

    def chain_replicate(self, command, key, value, replica_count):
        successor = self.successor
        if successor:
            self.log(f"Passing replication baton from {self.ip}:{self.port} to {successor.ip}:{successor.port} for key {key} and rc: {replica_count + 1}")
            return successor.forward_request(command=command, key=key, value=value, replica_count=replica_count + 1)

    def eventual_replicate(self, command, key, value, replica_count):
        successor = self.successor
        if successor:
            # simulate async delay
            time.sleep(0.1)
            self.log(f"Lazy forwarding to {successor.ip}:{successor.port} for key {key}")
            successor.forward_request(command=command, key=key, value=value, replica_count=replica_count + 1)

    def forward_request(self, command, key=None, value=None, replica_count=0,
                        hops=0, initial_node=None, replication_factor=None,
                        consistency=None, combined_transfer_keys=None):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.settimeout(2)
            client.connect((self.successor.ip, self.successor.port))

            message = command
            if key:
                message += f" {key}"
            if value:
                message += f" {value}"
            if replica_count > 0:
                message += f" {replica_count}"
            if hops > 0:
                message += f" {hops}"
            if initial_node is not None:
                message += f" {initial_node}"
            if replication_factor is not None and consistency is not None and initial_node is not None:
                message += f" {replication_factor} {consistency} {initial_node}"
            if combined_transfer_keys is not None:
                message += f" {combined_transfer_keys}"

            client.sendall(message.encode())

            # read large responses dynamically (useful for overlay or query *)
            response = []
            while True:
                try:
                    chunk = client.recv(1024).decode()
                    if not chunk:
                        break
                    response.append(chunk)
                except socket.timeout:
                    break
            return "".join(response)

    def responsible_for(self, key_hash):
        pred_id = self.predecessor.node_id if self.predecessor else None
        node_id = self.node_id

        # single node case
        if pred_id is None or pred_id == node_id:
            return True

        # usual case: predecessor < node_id
        if pred_id < node_id:
            return pred_id < key_hash <= node_id

        # wrap-around case: predecessor > node_id (means we're at the 0-boundary)
        return key_hash > pred_id or key_hash <= node_id


    def depart(self):
        self.log(f"Departing...")

        # Each node transfers all its primary and replica keys to its successor.
        # If the successor already holds these keys, their hop count is decremented.
        # Otherwise, the keys are inserted along with their values.
        # This process is then propagated to the following successors in the network.
        if self.successor.node_id != self.node_id:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                    client.settimeout(2)
                    client.connect((self.successor.ip, self.successor.port))
                    client.sendall(f"receive_keys {json.dumps(self.data)}".encode())
                    ack = client.recv(1024).decode()
                    if ack != "ACK":
                        self.log(f"{Fore.RED}ERROR: Successor {str(self.successor.node_id)[-4:]} did not confirm key transfer: {ack}{Style.RESET_ALL}")
                    else:
                        self.log(f"{Fore.GREEN}Successor {str(self.successor.node_id)[-4:]} did confirm key transfer!{Style.RESET_ALL}")
            except Exception as x:
                self.log(f"{Fore.RED}ERROR: Could not transfer keys to {self.successor.ip}:{self.successor.port}: {x}{Style.RESET_ALL}")

        # notify predecessor to update successor
        if self.predecessor.node_id != self.node_id:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                    client.settimeout(2)
                    client.connect((self.predecessor.ip, self.predecessor.port))
                    client.sendall(f"update_successor {self.successor.ip} {self.successor.port}".encode())
                    ack = client.recv(1024).decode()
                    if ack != "ACK":
                        self.log(f"{Fore.RED}ERROR: Predecessor {str(self.predecessor.node_id)[-4:]} did not acknowledge successor update: {ack}{Style.RESET_ALL}")
                    else:
                        self.log(f"{Fore.GREEN}Predecessor {str(self.predecessor.node_id)[-4:]} did acknowledge successor update!{Style.RESET_ALL}")
            except Exception as x:
                self.log(f"{Fore.RED}ERROR: Could not update predecessor at {self.predecessor.ip}:{self.predecessor.port}: {x}{Style.RESET_ALL}")

        # notify successor to update predecessor
        if self.successor.node_id != self.node_id:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                    client.settimeout(2)
                    client.connect((self.successor.ip, self.successor.port))
                    client.sendall(f"update_predecessor {self.predecessor.ip} {self.predecessor.port}".encode())
                    ack = client.recv(1024).decode()
                    if ack != "ACK":
                        self.log(f"{Fore.RED}ERROR: Successor {str(self.successor.node_id)[-4:]} did not acknowledge predecessor update: {ack}{Style.RESET_ALL}")
                    else:
                        self.log(f"{Fore.GREEN}Successor {str(self.successor.node_id)[-4:]} did acknowledge predecessor update!{Style.RESET_ALL}")
            except Exception as x:
                self.log(f"{Fore.RED}ERROR: Could not update successor at {self.successor.ip}:{self.successor.port}: {x}{Style.RESET_ALL}")

        self.log("Closing socket...")
        self.server_socket.close()
        self.log(f"Successfully departed from the Chord ring.")

    def find_successor(self, node_id):
        # single node in the ring
        if self.node_id == self.successor.node_id:
            return self

        # if the node ID fits between this node and its successor, return the successor
        if self.node_id < node_id <= self.successor.node_id:
            return self.successor

        # wrap-around case when IDs roll over the hash range
        if self.node_id > self.successor.node_id:
            if node_id > self.node_id or node_id <= self.successor.node_id:
                return self.successor          

        # forward request to next node
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect((self.successor.ip, self.successor.port))
                client.sendall(f"find_successor {node_id}".encode())
                response = client.recv(1024).decode()
                successor_ip, successor_port = response.split(":")
                return Node(successor_ip, int(successor_port))
        except:
            self.log(f"{Fore.RED}ERROR: Forwarding find_successor failed to {self.successor.ip}:{self.successor.port}{Style.RESET_ALL}")
            return self

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start a Chord Node")
    parser.add_argument("--ip", type=str, required=True, help="IP address of the node")
    parser.add_argument("--port", type=int, required=True, help="Port number of the node")
    parser.add_argument("--bootstrap", action="store_true", help="Whether this node is the bootstrap node")

    parser.add_argument("--replication_factor", type=int,
                        help="Number of times each piece of data is replicated across different nodes.")
    parser.add_argument("--consistency", type=str, choices=["chain", "eventual"],
                        help="Defines how updates are propagated across nodes. "
                             "'chain' ensures strict ordering and consistency, while 'eventual' allows faster but less strict consistency.")

    parser.add_argument("--bootstrap_ip", type=str, help="IP address of the bootstrap node")
    parser.add_argument("--bootstrap_port", type=int, help="Port number of the bootstrap node")

    args = parser.parse_args()

    if args.bootstrap:
        if args.replication_factor is None or args.consistency is None:
            print(f"{Fore.RED}Error: --replication_factor & --consistency are required when setting up a bootstrap node{Style.RESET_ALL}")
            exit(1)
        if args.bootstrap_ip or args.bootstrap_port:
            print(f"{Fore.YELLOW}Warning: --bootstrap_ip & --bootstrap_port are ignored since you are the bootstrap node.{Style.RESET_ALL}")
    else:
        if not args.bootstrap_ip or not args.bootstrap_port:
            print(f"{Fore.RED}Error: --bootstrap_ip & --bootstrap_port are required when you are not the bootstrap node{Style.RESET_ALL}")
            exit(1)
        if args.replication_factor or args.consistency:
            print(f"{Fore.RED}Error: --replication_factor & --consistency should not be set when you are not the bootstrap node{Style.RESET_ALL}")

    node = Node(ip=args.ip,
                port=args.port,
                bootstrap_ip=args.bootstrap_ip,
                bootstrap_port=args.bootstrap_port,
                bootstrap=args.bootstrap,
                replication_factor=args.replication_factor,
                consistency=args.consistency)

    # handle Control-C & gracefully depart
    def handle_exit(signum, frame):
        print(f"{Fore.YELLOW}\nGracefully departing the network...{Style.RESET_ALL}")
        node.depart()
        # ensure server socket is properly closed
        if hasattr(node, 'server_socket'):
            print("Closing server socket...")
            try:
                node.server_socket.shutdown(socket.SHUT_RDWR)
                node.server_socket.close()
            except OSError:
                pass
        # force exit to avoid threading shutdown errors
        os._exit(0)

    signal.signal(signal.SIGINT, handle_exit)


    threading.Thread(target=node.start_server).start()
