import readline
import tkinter as tk
from tkinter import scrolledtext, simpledialog, messagebox
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import threading
import time
from node import Node
import networkx as nx
import matplotlib.pyplot as plt
from utils import log
import os

BOOTSTRAP_IP = "127.0.0.1"
BOOTSTRAP_PORT = 5000
PREFIX = "[NETWORK]: "

class ChordGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Chord Network Simulator")

        self.network = ChordNetwork(BOOTSTRAP_IP, BOOTSTRAP_PORT, self)
        self.create_widgets()
        self.create_visualization()

    def create_widgets(self):
        button_frame = tk.Frame(self.root)
        button_frame.pack(pady=10)

        tk.Button(button_frame, text="Join Node", command=self.join_node).grid(row=0, column=0, padx=10)
        tk.Button(button_frame, text="Depart Node", command=self.depart_node).grid(row=0, column=1, padx=10)
        tk.Button(button_frame, text="Exit", command=self.exit_network).grid(row=0, column=2, padx=10)

        self.log_box = scrolledtext.ScrolledText(self.root, width=80, height=10, wrap=tk.WORD)
        self.log_box.pack(pady=10)
        self.log_message("Chord Network Initialized.")

    def create_visualization(self):
        """ Create a Matplotlib canvas inside Tkinter to display the network overlay """
        self.fig, self.ax = plt.subplots(figsize=(6, 4))
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.root)
        self.canvas.get_tk_widget().pack()

        self.network.visualize_chord_ring(ax=self.ax)
        self.canvas.draw()

    def log_message(self, message):
        """ Append message to the log box """
        self.log_box.insert(tk.END, message + "\n")
        self.log_box.yview(tk.END)

    def join_node(self):
        """ Joins a new node dynamically """
        # port = simpledialog.askinteger("Join Node", "Enter Port Number:", minvalue=5001, maxvalue=65535)
        port = self.network.next_port
        self.network.next_port += 1

        if port:
            threading.Thread(target=self.network.join_node, args=(BOOTSTRAP_IP, port)).start()

    def depart_node(self):
        """ Departs a node based on user input """
        short_id = simpledialog.askstring("Depart Node", "Enter Node Short ID:")  # Keeps leading zeros
        if short_id:
            short_id = short_id.zfill(4)  # Convert to string before padding
            found_node = None

            # Find node by matching last 4 digits
            for node_id in self.network.nodes.keys():
                if str(node_id)[-4:] == short_id:
                    found_node = node_id
                    break

            if found_node:
                threading.Thread(target=self.network.depart_node, args=(found_node,)).start()
                self.log_message(f"Departing Node {found_node}...")
            else:
                self.log_message(f"No node found with Short ID: {short_id}.")

    def exit_network(self):
        """ Cleanly exits the Chord network and waits for all nodes to depart """
        if messagebox.askyesno("Exit", "Are you sure you want to exit?"):

            self.log_message("Will initiate deportation...")
            self.network.depart_all_nodes()

            time.sleep(0.2)

            self.log_message("All nodes have departed. Exiting...")
            self.root.destroy()

            os._exit(0)

    def update_visualization(self):
        """ Updates the network visualization in the GUI """
        self.ax.clear()  # Clear old graph
        self.network.visualize_chord_ring(ax=self.ax)  # Redraw the overlay
        self.canvas.draw()  # Refresh the canvas

class ChordNetwork:
    def __init__(self, bootstrap_ip, bootstrap_port, gui):
        self.gui = gui  # Reference to the GUI for visualization updates
        self.bootstrap_node = Node(bootstrap_ip, bootstrap_port, bootstrap=True)
        self.nodes = {self.bootstrap_node.node_id: self.bootstrap_node}
        self.next_port = bootstrap_port + 1
        threading.Thread(target=self.bootstrap_node.start_server).start()

    def join_node(self, ip, port):
        """ Joins a new node and updates the network """
        new_node = Node(ip, port)
        threading.Thread(target=new_node.start_server).start()
        new_node.join(self.bootstrap_node.ip, self.bootstrap_node.port)
        self.nodes[new_node.node_id] = new_node
        if self.gui:
            self.gui.log_message(f"Joining node {new_node.node_id} at {ip}:{port}")
        log(PREFIX, f"Node {new_node.node_id} joined at {ip}:{port}")

        if self.gui:
            self.gui.update_visualization()  # Update overlay

    def depart_node(self, node_id):
        departed = False
        if node_id in self.nodes:
            node = self.nodes[node_id]
            node.depart()
            del self.nodes[node_id]
            log(PREFIX, f"Node {node_id} departed")
            departed = True
        else:
            log(PREFIX, f"Node {node_id} not found")
            departed = False

        if self.gui:
            self.gui.update_visualization()
        return departed

    def visualize_chord_ring(self, ax):
        """ Draw the Chord ring in the given Matplotlib Axes """
        nodes_sorted = sorted(self.nodes.items(), reverse=True)

        G = nx.DiGraph()
        labels = {}

        for node_id, node in nodes_sorted:
            successor = node.successor.node_id if node.successor else node_id
            G.add_edge(node_id, successor)
            labels[node_id] = f"{str(node_id)[-4:]}|{str(node.port)[-2:]}"

        # Increase figure size and margins to avoid cropping
        ax.set_xlim(-1.2, 1.2)  # Increase limits for more space
        ax.set_ylim(-1.2, 1.2)

        # Add extra padding to avoid clipping
        ax.margins(0.2)

        # Draw network with better spacing
        pos = nx.circular_layout(G)  # Use circular layout
        nx.draw(G, pos, with_labels=True, labels=labels, node_size=800, node_color="lightblue",
                edge_color="gray", font_size=6, arrowsize=10, ax=ax)

        # Ensure all labels are fully visible
        for node, (x, y) in pos.items():
            ax.text(x, y, labels[node], fontsize=6, ha='center', va='center')

    def depart_all_nodes(self):
        """ Departs all nodes from the network """
        log(PREFIX, "Departing all nodes...")
        for node_id in list(self.nodes.keys()):
            # self.gui.log_message(f"Departing node {node_id}...")
            departed = self.depart_node(node_id)
            if self.gui:
                self.gui.log_message(f"Departed Node: {str(node_id)[-4:]}" if departed else f"Failed to depart Node: {str(node_id)[-4:]}")
                self.gui.root.update()
            else:
                log(PREFIX, f"Departed Node: {str(node_id)[-4:]}" if departed else f"Failed to depart Node: {str(node_id)[-4:]}")


    def cli(self):
        log(PREFIX, "Chord Network initialized with bootstrap node.")
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
                    short_id = command[1].zfill(4)  # Ensure it's 4 digits with leading zeros if necessary
                    found_node = None
                    for node_id in self.nodes.keys():
                        if str(node_id)[-4:] == short_id:
                            found_node = node_id
                            break
                    if found_node:
                        self.depart_node(found_node)
                        log(PREFIX, f"Departed Node: {short_id}")
                    else:
                        log(PREFIX, f"No node found with Short ID: {short_id}")
                elif action == "overlay":
                    fig, ax = plt.subplots(figsize=(6, 4))  # Create new figure
                    self.visualize_chord_ring(ax)  # Draw network graph
                    plt.show(block=False)  # ✅ Make it non-blocking
                elif action == "help":
                    log(PREFIX, "Commands: join <ip> <port>, depart <node_id>, overlay, help, exit")
                elif action == "exit":
                    log(PREFIX, "Shutting down the Chord Network...")
                    self.depart_all_nodes()
                    log(PREFIX, "All nodes have departed. Exiting...")
                    os._exit(0)
                else:
                    log(PREFIX, "Invalid command. Type 'help' for usage.")

            except KeyboardInterrupt:
                log(PREFIX, "\nExiting Chord Network.")
                self.depart_all_nodes()
                log(PREFIX, "All nodes have departed. Exiting...")
                break

import sys

if __name__ == "__main__":
    mode = "--gui"

    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()

    if mode == "--cli":
        log(PREFIX, "Starting Chord Network in CLI mode...")
        network = ChordNetwork(BOOTSTRAP_IP, BOOTSTRAP_PORT, gui=None)
        network.cli()
    elif mode == "--gui":
        log(PREFIX, "Starting Chord Network in GUI mode...")
        root = tk.Tk()
        app = ChordGUI(root)
        root.mainloop()
    else:
        print("Invalid mode! Use '--cli' for CLI mode or '--gui' for GUI mode.")
