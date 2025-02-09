import threading
import time
from node import Node
import readline  # Enables history and arrow navigation

class ChordNetwork:
    def __init__(self, bootstrap_ip, bootstrap_port):
        self.bootstrap_node = Node(bootstrap_ip, bootstrap_port)
        self.nodes = {self.bootstrap_node.node_id: self.bootstrap_node}
        threading.Thread(target=self.bootstrap_node.start_server).start()

    def join_node(self, ip, port):
        new_node = Node(ip, port)
        threading.Thread(target=new_node.start_server).start()
        new_node.join(self.bootstrap_node.ip, self.bootstrap_node.port)
        self.nodes[new_node.node_id] = new_node
        print(f"Node {new_node.node_id} joined at {ip}:{port}")

    def depart_node(self, node_id):
        if node_id in self.nodes:
            node = self.nodes[node_id]
            node.depart()
            del self.nodes[node_id]
            print(f"Node {node_id} departed")
        else:
            print(f"Node {node_id} not found")

    def display_overlay(self):
        print("\nChord Network Overlay:")
        for node_id, node in sorted(self.nodes.items()):
            successor = node.successor.node_id if node.successor else "None"
            predecessor = node.predecessor.node_id if node.predecessor else "None"
            print(f"Node ID: {node_id}, Successor: {successor}, Predecessor: {predecessor}")
        print()

    def depart_all_nodes(self):
        print("\nDeparting all nodes...")
        # Collect the nodes into a list to avoid modifying the dictionary while iterating
        for node_id in list(self.nodes.keys()):
            self.depart_node(node_id)

    def start(self):
        print("Chord Network initialized with bootstrap node.")
        time.sleep(2)
        readline.set_history_length(100)  # Store up to 100 commands in history
        while True:
            try:
                command = input("> ").strip().split()
                if len(command) == 0:
                    continue

                action = command[0].lower()
                if action == "join" and len(command) == 3:
                    ip, port = command[1], int(command[2])
                    self.join_node(ip, port)
                elif action == "depart" and len(command) == 2:
                    node_id = int(command[1])
                    self.depart_node(node_id)
                elif action == "overlay":
                    self.display_overlay()
                elif action == "help":
                    print("Commands: join <ip> <port>, depart <node_id>, overlay, help, exit")
                elif action == "exit":
                    print("Shutting down the Chord Network...")
                    self.depart_all_nodes()
                    print("All nodes have departed. Exiting...")
                    break
                else:
                    print("Invalid command. Type 'help' for usage.")

            except KeyboardInterrupt:
                print("\nExiting Chord Network.")
                self.depart_all_nodes()
                print("All nodes have departed. Exiting...")
                break

if __name__ == "__main__":
    network = ChordNetwork("127.0.0.1", 5000)
    network.start()
