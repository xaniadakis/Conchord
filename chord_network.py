import readline
import tkinter as tk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import threading
import time
from node import Node
import networkx as nx
import matplotlib.pyplot as plt
from utils import log
import os
from tkinter import scrolledtext, simpledialog, messagebox, Toplevel
import signal
import argparse

BOOTSTRAP_IP = "127.0.0.1"
BOOTSTRAP_PORT = 5000
REPLICATION_FACTOR = 1
PREFIX = "[NETWORK]: "

class ChordGUI:
    def __init__(self, root, bootstrap_ip, bootstrap_port, startup_nodes, replication_factor, consistency):
        self.root = root
        self.root.title("Chordify Network")
        self.root.protocol("WM_DELETE_WINDOW", self.exit_network)
        self.network = ChordNetwork(bootstrap_ip=bootstrap_ip,
                                    bootstrap_port=bootstrap_port,
                                    startup_nodes=startup_nodes,
                                    replication_factor=replication_factor,
                                    consistency=consistency,
                                    gui=self)
        self.bootstrap_ip = bootstrap_ip
        self.create_widgets()
        self.create_visualization()

    def create_widgets(self):
        button_frame = tk.Frame(self.root)
        button_frame.pack(pady=10)

        tk.Button(button_frame, text="Join Node", command=self.join_node).grid(row=0, column=0, padx=10)
        tk.Button(button_frame, text="Depart Node", command=self.depart_node).grid(row=0, column=1, padx=10)
        tk.Button(button_frame, text="Refresh Graph", command=self.update_visualization).grid(row=0, column=2, padx=10)
        tk.Button(button_frame, text="Exit", command=self.exit_network).grid(row=0, column=3, padx=10)
        tk.Button(button_frame, text="Show Node IDs", command=self.show_node_ids).grid(row=0, column=4, padx=10)


        self.log_box = scrolledtext.ScrolledText(self.root, width=80, height=10, wrap=tk.WORD)
        self.log_box.pack(pady=10)
        self.log_message("Chord Network Initialized.")

    def show_node_ids(self):
        """Displays the list of existing node IDs in the log box."""
        node_ids = self.network.get_node_ids()
        self.log_message(f"Existing Node IDs: {node_ids}")

    def create_visualization(self):
        """ Create a Matplotlib canvas inside Tkinter to display the network overlay """
        self.fig, self.ax = plt.subplots(figsize=(6, 4))
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.root)
        self.canvas.get_tk_widget().pack()

        self.network.visualize_chord_ring(ax=self.ax)
        self.canvas.draw()
        self.canvas.mpl_connect("button_press_event", self.on_node_click)

    def on_node_click(self, event):
        if event.inaxes != self.ax:
            return
        if not hasattr(self.network, 'node_positions'):
            return
        threshold = 0.1
        for node_id, pos in self.network.node_positions.items():
            dx = event.xdata - pos[0]
            dy = event.ydata - pos[1]
            if (dx ** 2 + dy ** 2) ** 0.5 < threshold:
                node_obj = self.network.nodes[node_id]
                keys = getattr(node_obj, 'data', None)
                keys_str = "\n".join(map(str, keys)) if keys else "No keys"

                key_window = Toplevel(self.root)
                key_window.title(f"Node {str(node_id)[-4:]} Keys")
                key_window.geometry("400x300")

                text_widget = scrolledtext.ScrolledText(key_window, width=50, height=15, wrap=tk.WORD)
                text_widget.pack(pady=10, padx=10)
                text_widget.insert(tk.END, keys_str)
                text_widget.config(state=tk.DISABLED)
                break

    def log_message(self, message):
        """ Append message to the log box """
        self.log_box.insert(tk.END, message + "\n")
        self.log_box.yview(tk.END)

    def join_node(self):
        """ Joins a new node dynamically """
        port = self.network.next_port
        self.network.next_port += 1

        if port:
            threading.Thread(target=self.network.join_node, args=(self.bootstrap_ip, port)).start()

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

    def exit_network(self, ask=False):
        """ Cleanly exits the Chord network and waits for all nodes to depart """
        try:
            if ask and not messagebox.askyesno("Exit", "Are you sure you want to exit?"):
                return

            self.log_message("Will initiate deportation...")
            self.network.depart_all_nodes()

            time.sleep(0.2)

            self.log_message("All nodes have departed. Exiting...")
            self.root.destroy()

            os._exit(0)
        except Exception as e:
            self.log_message(f"Error during exit: {e}")
            os._exit(1)

    def update_visualization(self):
        """ Updates the network visualization in the GUI """
        self.ax.clear()  # Clear old graph
        self.network.visualize_chord_ring(ax=self.ax)  # Redraw the overlay
        self.canvas.draw()  # Refresh the canvas

class ChordNetwork:
    def __init__(self, bootstrap_ip, bootstrap_port, startup_nodes, replication_factor, consistency, gui):
        self.gui = gui
        self.bootstrap_node = Node(ip=bootstrap_ip,
                                   port=bootstrap_port,
                                   bootstrap=True,
                                   replication_factor=replication_factor,
                                   consistency=consistency)
        self.bootstrap_ip = bootstrap_ip
        self.replication_factor = replication_factor
        self.consistency = consistency

        self.nodes = {self.bootstrap_node.node_id: self.bootstrap_node}
        self.next_port = bootstrap_port + 1
        threading.Thread(target=self.bootstrap_node.start_server).start()

        if startup_nodes > 0:
            threading.Thread(target=self.init_nodes_async, args=(startup_nodes,), daemon=True).start()

    def init_nodes_async(self, num_nodes):
        """ Asynchronously join `num_nodes` to the network without blocking GUI """
        for _ in range(num_nodes):
            self.join_node(ip=self.bootstrap_ip,
                           port=self.next_port,
                           replication_factor=self.replication_factor,
                           consistency=self.consistency,
                           silent=True)
            self.next_port += 1
            time.sleep(0.3)
            if self.gui:
                self.gui.update_visualization()

    def get_node_ids(self):
        """Returns a list of existing node IDs."""
        return sorted(list(self.nodes.keys()))

    def join_node(self, ip, port, replication_factor, consistency, silent=False):
        """ Joins a new node and updates the network """
        new_node = Node(ip=ip, port=port, replication_factor=replication_factor, consistency=consistency)
        threading.Thread(target=new_node.start_server).start()
        new_node.join(self.bootstrap_node.ip, self.bootstrap_node.port)
        self.nodes[new_node.node_id] = new_node
        if not silent and self.gui:
            self.gui.log_message(f"Joining node {new_node.node_id} at {ip}:{port}")
        log(PREFIX, f"Node {new_node.node_id} joined at {ip}:{port}")

        if not silent and self.gui:
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

    import networkx as nx
    import matplotlib.pyplot as plt

    def visualize_chord_ring(self, ax):
        """ Draw the Chord ring in the given Matplotlib Axes """
        nodes_sorted = sorted(self.nodes.items(), key=lambda x: x[1].node_id)
        G = nx.DiGraph()
        labels = {}
        node_colors = []
        text_colors = {}

        for node_id, node in nodes_sorted:
            successor = node.successor.node_id if node.successor else node_id
            G.add_edge(node_id, successor)
            labels[node_id] = f"{str(node_id)[-4:]}|{str(node.port)[-2:]}"

            # Assign colors
            if node.bootstrap_node:
                node_colors.append("#BE3144")
                text_colors[node_id] = "white"
            else:
                node_colors.append("lightblue")
                text_colors[node_id] = "black"

        ax.set_xlim(-1.2, 1.2)
        ax.set_ylim(-1.2, 1.2)
        ax.margins(0.2)

        pos = nx.circular_layout(G)
        self.node_positions = pos

        # Draw nodes and edges
        nx.draw(G, pos, with_labels=False, node_size=800, node_color=node_colors, edge_color="gray",
                font_size=6, arrowsize=10, ax=ax)

        # Draw labels with specific text colors
        for node, (x, y) in pos.items():
            ax.text(x, y, labels[node], fontsize=6, color=text_colors[node],
                    ha='center', va='center', bbox=dict(facecolor='none', edgecolor='none', pad=0))

        # Show key counts
        for node, (x, y) in pos.items():
            key_count = len(getattr(self.nodes[node], 'data', []))
            ax.text(x + 0.12, y + 0.15, str(key_count), fontsize=6, color='white',
                    ha='center', va='center', bbox=dict(facecolor='black', edgecolor='black',
                                                        boxstyle='round,pad=0.5', alpha=0.75))

    def depart_all_nodes(self):
        """ Departs all nodes from the network """
        log(PREFIX, "Departing all nodes...")
        for node_id in list(self.nodes.keys()):
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
        try:
            while True:
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
            log(PREFIX, "\nReceived KeyboardInterrupt. Exiting Chord Network gracefully.")
            self.depart_all_nodes()
            log(PREFIX, "All nodes have departed. Exiting...")
            os._exit(0)


if __name__ == "__main__":
    mode = "--gui"

    parser = argparse.ArgumentParser(description="Start a Chord Network")
    parser.add_argument("-m", "--mode", choices=["cli", "gui"], required=True, help="Mode to run the Chord network")
    parser.add_argument("-n", "--num_nodes", type=int, default=0, help="Number of startup nodes (default: 0)")
    parser.add_argument("-r", "--replication_factor", type=int, default=REPLICATION_FACTOR, help=f"Replication factor (default: {REPLICATION_FACTOR})")
    parser.add_argument("-c", "--consistency", choices=["chain", "eventual"], default="chain", help="Consistency model (default: chain)")
    parser.add_argument("-i", "--bootstrap_ip", type=str, default=BOOTSTRAP_IP, help=f"Bootstrap node IP address (default: {BOOTSTRAP_IP})")
    parser.add_argument("-p", "--bootstrap_port", type=int, default=BOOTSTRAP_PORT, help=f"Bootstrap node port (default: {BOOTSTRAP_PORT})")
    args = parser.parse_args()

    if args.mode == "cli":
        log(PREFIX, "Starting Chord Network in CLI mode...")
        network = ChordNetwork(bootstrap_ip=args.bootstrap_ip,
                               bootstrap_port=args.bootstrap_port,
                               startup_nodes=args.num_nodes - 1,
                               replication_factor=args.replication_factor,
                               consistency=args.consistency,
                               gui=None)
        network.cli()
    elif args.mode == "gui":
        log(PREFIX, "Starting Chord Network in GUI mode...")
        root = tk.Tk()
        app = ChordGUI(root=root,
                       bootstrap_ip=args.bootstrap_ip,
                       bootstrap_port=args.bootstrap_port,
                       startup_nodes=args.num_nodes - 1,
                       replication_factor=args.replication_factor,
                       consistency=args.consistency)

        # handle Control-C taps and gracefully exit in a fast manner
        exit_requested = False
        def handle_sigint(signum, frame):
            global exit_requested
            exit_requested = True
        signal.signal(signal.SIGINT, handle_sigint)
        def check_exit():
            if exit_requested:
                app.exit_network(ask=False)
            else:
                root.after(100, check_exit)
        root.after(100, check_exit)

        root.mainloop()
    else:
        print("Invalid mode! Use '--cli' for CLI mode or '--gui' for GUI mode.")
