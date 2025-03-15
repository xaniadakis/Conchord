import pandas as pd
import streamlit as st
from numpy.f2py.auxfuncs import throw_error
from streamlit_option_menu import option_menu
import datetime
import socket
import matplotlib.pyplot as plt
import networkx as nx
import json
from tqdm import tqdm  # You may need to install this with pip if not installed
import os
import time
import matplotlib.pyplot as plt
import subprocess

from chord_network import BOOTSTRAP_IP

plt.style.use("dark_background")

# ---- PAGE CONFIG ----
st.set_page_config(page_title="ConChord", page_icon="🎼", layout="wide")
# ---- GLOBAL CSS FOR FULL-WIDTH BUTTONS ----
st.markdown(
    """
    <style>
        div.stButton > button {
            width: 100% !important;
        }
    </style>
    """,
    unsafe_allow_html=True
)

# ---- SESSION STATE INITIALIZATION ----
if "action" not in st.session_state:
    st.session_state["action"] = None
if "key" not in st.session_state:
    st.session_state["key"] = ""
if "value" not in st.session_state:
    st.session_state["value"] = ""
if "query_key" not in st.session_state:
    st.session_state["query_key"] = ""
if "delete_key" not in st.session_state:
    st.session_state["delete_key"] = ""

# Global dictionary to store node information
st.session_state["node_info"] = {}
VM_MAPPING = {
    "10.0.9.91": 1,
    "10.0.9.86": 2,
    "10.0.9.176": 3,
    "10.0.9.31": 4,
    "10.0.9.160": 5
}
import subprocess
import os
import time
import socket

BOOTSTRAP_IP = '127.0.0.1'
BOOTSTRAP_PORT = 5000

def ssh_run_node(vm_number, ip, port, is_bootstrap=False,
                 replication_factor=3, consistency="chain",
                 bootstrap_ip=BOOTSTRAP_IP, bootstrap_port=BOOTSTRAP_PORT):
    """Start a new node (locally or remotely via SSH) and confirm when it's running."""
    print(f"[INFO] Starting node: IP={ip}, Port={port}, VM={vm_number}, Bootstrap={is_bootstrap}")

    # Ensure absolute path to node.py
    node_path = os.path.expanduser("~/conchord/node.py") if int(vm_number) > 0 else "./node.py"
    log_path = os.path.expanduser(f"~/conchord/logs/node_{port}.log") if int(vm_number) > 0 else f"./logs/node_{port}.log"

    # Ensure logs directory exists
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    # Construct the correct command
    if is_bootstrap:
        command = f"nohup python3 {node_path} --ip {ip} --port {port} --bootstrap --replication_factor {replication_factor} --consistency {consistency} > {log_path} 2>&1 &"
    else:
        if bootstrap_ip is None or bootstrap_port is None:
            print(f"[ERROR] Non-bootstrap nodes require bootstrap_ip and bootstrap_port.")
            return f"Error: Missing bootstrap IP/port for non-bootstrap node."
        command = f"nohup python3 {node_path} --ip {ip} --port {port} --bootstrap_ip {bootstrap_ip} --bootstrap_port {bootstrap_port} > {log_path} 2>&1 &"

    # Decide whether to run locally or via SSH
    if int(vm_number) < 0:
        print(f"[INFO] Running locally on this machine.")
        shell_command = f"/bin/bash -c '{command}'"
    else:
        vm_alias = f"team_2-vm{vm_number}"
        print(f"[INFO] Running remotely on {vm_alias} via SSH.")
        shell_command = f"ssh {vm_alias} '/bin/bash -c \"{command}\"'"

    print(f"[DEBUG] Executing command: {shell_command}")

    try:
        # Run command in background
        result = subprocess.run(shell_command, shell=True, executable="/bin/bash", capture_output=True, text=True)

        if result.returncode == 0:
            print(f"[INFO] Node process started, checking if it's running...")

            # Wait and check if the node is reachable
            for i in range(10):
                time.sleep(1)
                if is_node_running(ip, port):
                    print(f"[SUCCESS] Node is running on {ip}:{port} ✅")
                    return f"Success: Node started and running on {ip}:{port}"
                print(f"[INFO] Waiting for node to start... ({i+1}/10)")

            print(f"[ERROR] Node process started but not responding on {ip}:{port}.")
            return f"Error: Node process started but not responding."

        else:
            print(f"[ERROR] Node failed to start. Return code: {result.returncode}")
            print(f"[STDERR] {result.stderr.strip()}")
            return f"Error: Node failed to start. Return code: {result.returncode}, Error: {result.stderr.strip()}"

    except Exception as e:
        print(f"[EXCEPTION] SSH Connection Error: {e}")
        return f"SSH Connection Error: {e}"

def is_node_running(ip, port):
    """Check if the node is listening on the given IP and port."""
    try:
        with socket.create_connection((ip, port), timeout=1):
            return True
    except (socket.timeout, ConnectionRefusedError):
        return False

# ---- FUNCTION: SEND COMMAND TO CHORD NETWORK ----
def send_command(command):
    """Send a command to the bootstrap node and return the response."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((BOOTSTRAP_IP, BOOTSTRAP_PORT))  # Connect to bootstrap node
            client.sendall(command.encode())

            response = []
            while True:
                chunk = client.recv(4096).decode()
                if not chunk:
                    break
                response.append(chunk)

            return "".join(response)  # Join response chunks

    except Exception as e:
        return f"Error: {e}"

# ---- FUNCTION: FETCH OVERLAY DATA ----
def fetch_nodes():
    """Fetch the entire overlay from the bootstrap node."""
    try:
        response = send_command("overlay")  # Send overlay request
        if not response.strip():
            return {"error": "Empty response from network"}

        nodes = json.loads(response)  # Parse JSON safely
        print(nodes)
        # Store node details globally for later use (departing nodes)
        st.session_state["node_info"] = {
            str(node_id): {
                "ip": details.get("ip"),
                "port": details.get("port"),
                "vm": VM_MAPPING.get(details.get("ip"), -1)
            }
            for node_id, details in nodes.items()
        }
        return nodes

    except json.JSONDecodeError:
        return {"error": "Invalid JSON format from server"}
    except Exception as e:
        return {"error": str(e)}

def fetch_data_from_node(node_id):
    """Fetch stored data from a specific node."""
    try:
        command = f'get_data {node_id}'
        response = send_command(command)  # Send request to the node
        print(response)
        if not response.strip():
            return {"error": "Empty response from node"}

        node_data = json.loads(response)  # Parse JSON response
        return node_data

    except json.JSONDecodeError:
        return {"error": "Invalid JSON format from node"}
    except Exception as e:
        return {"error": str(e)}

# ---- FUNCTION: VISUALIZE CHORD NETWORK ----
def visualize_chord_ring():
    """Draw the Chord ring with correct node connections and key counts above nodes."""
    nodes = fetch_nodes()

    if "error" in nodes:
        st.error(f"No network to visualize")
        print(f"Failed to fetch network: {nodes['error']}")
        return

    fig, ax = plt.subplots(figsize=(7, 5))
    G = nx.DiGraph()
    labels = {}
    node_colors = {}
    text_colors = {}

    # Ensure node IDs are always strings
    nodes = {str(node_id): details for node_id, details in nodes.items()}

    # Explicitly add all nodes first
    for node_id, details in nodes.items():
        G.add_node(node_id)
        labels[node_id] = f"{node_id[-4:]} | {str(details.get('port', '??'))[-2:]}"  # Short ID | Port

        # Assign colors
        if details.get("is_bootstrap", False):
            node_colors[node_id] = "#BE3144"  # Bootstrap node (Red)
            text_colors[node_id] = "white"
        else:
            node_colors[node_id] = "lightblue"  # Normal nodes (Blue)
            text_colors[node_id] = "black"

    # Add edges
    for node_id, details in nodes.items():
        successor = str(details.get("successor"))
        if successor in nodes:
            G.add_edge(node_id, successor)
        else:
            st.warning(f"Node {node_id} has an unknown successor {successor}, skipping edge.")

    # **Handle layout for 2 nodes** (avoids overlapping in `nx.circular_layout`)
    if len(G.nodes) == 2:
        pos = {
            list(G.nodes)[0]: (-1, 0),
            list(G.nodes)[1]: (1, 0)
        }
        show_key_counts = False  # Don't show key counts for 2 nodes
    else:
        pos = nx.circular_layout(G)
        show_key_counts = True  # Show key counts normally

    print("Nodes:", nodes)
    print("Edges:", list(G.edges))

    # Draw nodes and edges
    nx.draw(G, pos, with_labels=False, node_size=800, node_color=[node_colors[n] for n in G.nodes],
            edge_color="gray", font_size=6, arrowsize=10, ax=ax)

    # Draw labels with specific text colors
    for node, (x, y) in pos.items():
        ax.text(x, y, labels[node], fontsize=6, color=text_colors[node],
                ha='center', va='center', bbox=dict(facecolor='none', edgecolor='none', pad=0))

    # Show key counts **ONLY if more than 2 nodes**
    if show_key_counts:
        for node, (x, y) in pos.items():
            key_count = nodes[node].get("key_count", 0)
            ax.text(x, y + 0.1, str(key_count), fontsize=6, color='white',
                    ha='center', va='center', bbox=dict(facecolor='black', edgecolor='black',
                                                        boxstyle='round,pad=0.5', alpha=0.75))

    st.pyplot(fig)


def process_insert_directory(directory):
    """Process a directory containing insert files after fetching network configuration."""
    if not os.path.exists(directory) or not os.path.isdir(directory):
        return False, 0, None, 0  # Return failure, 0 time, and no config

    # Fetch network configuration
    config_response = send_command("get_network_config").strip()

    if ":" in config_response:
        replication_factor, consistency = config_response.split(":")
        network_config = f"Replication Factor: {replication_factor}, Consistency: {consistency}"
    else:
        network_config = "Failed to fetch network configuration"

    insert_files = sorted(f for f in os.listdir(directory) if f.startswith("insert_") and f.endswith(".txt"))

    total_files = len(insert_files)
    if total_files == 0:
        return False, 0, network_config, 0  # No files found

    start_time = time.time()

    key_counter = 0
    for filename in tqdm(insert_files, desc="Processing Files", unit="file", ncols=80):
        filepath = os.path.join(directory, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                value = filename.split('_')[1]  # Extract number part
                keys = [line.strip() for line in file if line.strip()]

                for key in tqdm(keys, desc=f"  Inserting keys from {filename}", unit="key", leave=False, ncols=80):
                    key_counter += 1
                    command = f"insert \"{key}\" {value}"
                    response = send_command(command)

        except Exception:
            return False, 0, network_config, 0  # Error during processing

    elapsed_time = time.time() - start_time
    return True, elapsed_time, network_config, key_counter

# ---- SIDEBAR NAVIGATION ----
with st.sidebar:
    selected = option_menu(
        menu_title="Navigation",
        options=["Operations", "Overlay", "Experiments"],
        icons=["database", "globe", "bi-gear"],
        menu_icon="cast",
        default_index=0,
    )


# ---- DATABASE OPERATIONS PAGE ----
if selected == "Operations":
    st.markdown("<h2 style='text-align: center;'>Operations</h2>", unsafe_allow_html=True)

    st.subheader("Choose an action:")
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        if st.button("Insert"):
            st.session_state["action"] = "insert"
    with col2:
        if st.button("Query"):
            st.session_state["action"] = "query"
    with col3:
        if st.button("Delete"):
            st.session_state["action"] = "delete"
    with col4:
        if st.button("Help"):
            st.session_state["action"] = "help"
    with col5:
        if st.button("Batch Insert"):
            st.session_state["action"] = "batch_insert"
    with col6:
        if st.button("Reset"):
            st.session_state["action"] = "reset_config"

    # ---- INSERT ACTION ----
    if st.session_state["action"] == "insert":
        st.markdown("<h3>Insert Data</h3>", unsafe_allow_html=True)

        key_input = st.text_input("Enter Key:", key="key")
        value_input = st.text_input("Enter Value:", key="value")

        if st.button("Submit Insert"):
            if key_input.strip() and value_input.strip():
                command = f'insert "{key_input}" {value_input}'
                response = send_command(command)
                st.markdown(
                    f"""
                    <div style='display: flex; justify-content: center;'>
                        <div style='width:20%; padding:10px; margin-bottom:15px; border-radius:5px; background-color:#d4edda; 
                                    color:#155724; font-weight:bold; font-size:17px; text-align: center;'>
                            {response}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            else:
                st.error("Please enter both a key and a value.")

    # ---- QUERY ACTION ----
    elif st.session_state["action"] == "query":
        st.markdown("<h3>Query Data</h3>", unsafe_allow_html=True)
        query_input = st.text_input("Enter Key to Query:", key="query_key")

        if st.button("Submit Query"):
            if query_input.strip():
                command = f'query "{query_input}"'
                response = send_command(command)

                st.markdown(
                    f"""
                    <div style='display: flex; justify-content: center;'>
                        <div style='width:20%; padding:10px; margin-bottom:15px; border-radius:5px; background-color:#d4edda; 
                                    color:#155724; font-weight:bold; font-size:17px; text-align: center;'>
                            {response}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            else:
                st.error("Please enter a key.")

        if st.button('Query ☆'):
            command = 'query ☆'
            response = send_command(command)

            if not response.strip():
                st.warning("Empty response from network.")
            else:
                try:
                    data = json.loads(response)
                    if not data:
                        st.info("No stored data found.")
                    else:
                        formatted_data = [{"Key": k, "Value": v} for k, v in data.items()]
                        st.table(formatted_data)  # Display as a structured table
                        # Use st.dataframe for full-width display
                        # st.dataframe(formatted_data, use_container_width=True)
                except json.JSONDecodeError:
                    st.error("Invalid JSON format in response.")

    # ---- DELETE ACTION ----
    elif st.session_state["action"] == "delete":
        st.markdown("<h3>Delete Data</h3>", unsafe_allow_html=True)

        col1, col2 = st.columns([3, 1])
        with col1:
            delete_input = st.text_input("Enter Key to Delete:", key="delete_key")

        with col2:
            st.markdown(
                """
                <style>
                div.stButton > button {
                    height: 36px !important; /* Match text_input height */
                    margin-top: 12.5px !important; /* Move button slightly down */
                    width: 100%;
                }
                </style>
                """,
                unsafe_allow_html=True
            )
            if st.button("Submit Delete"):
                if delete_input.strip():
                    command = f'delete "{delete_input}"'
                    response = send_command(command)
                    st.success(f"Response: {response}")
                else:
                    st.error("Please enter a key.")

    # ---- HELP ACTION ----
    elif st.session_state["action"] == "help":
        st.markdown("<h3>Help</h3>", unsafe_allow_html=True)
        st.info("Available Commands:")
        st.write("- **insert** `<key> <value>` - Add a key-value pair.")
        st.write("- **query** `<key>` - Retrieve a value by key.")
        st.write("- **delete** `<key>` - Remove a key-value pair.")
        st.write("- **help** - Show available commands.")

    # ---- BATCH INSERT ACTION ----
    if st.session_state.get("action") == "batch_insert":
        st.markdown("<h3>Batch Insert</h3>", unsafe_allow_html=True)

        directory = "insert"
        success, elapsed_time, network_config, key_counter = process_insert_directory(directory)  # Capture time & config

        if success:
            st.markdown(
                f"""
                <div style='display: flex; justify-content: center;'>
                    <div style='width:60%; padding:10px; margin-bottom:15px; border-radius:5px; background-color:#d4edda; 
                                color:#155724; font-weight:bold; font-size:17px; text-align: center;'>
                        Success: Batch insert of {key_counter} total keys processing completed in {elapsed_time:.2f} seconds.<br>
                        <strong>Network Configuration:</strong> {network_config}
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                f"""
                <div style='display: flex; justify-content: center;'>
                    <div style='width:60%; padding:10px; margin-bottom:15px; border-radius:5px; background-color:#f8d7da; 
                                color:#721c24; font-weight:bold; font-size:17px; text-align: center;'>
                        Failure: Batch insert encountered errors.<br>
                        <strong>Network Configuration:</strong> {network_config}
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )

    # ---- RESET CONFIGURATION ACTION ----
    if st.session_state.get("action") == "reset_config":
        st.markdown("<h3>Reset Configuration</h3>", unsafe_allow_html=True)

        col7, col8 = st.columns([3, 2])

        with col7:
            new_replication_factor = st.text_input("New Replication Factor:", key="new_replication_factor")

        with col8:
            new_consistency_type = st.selectbox("New Consistency Type:", ["chain", "eventual"], key="new_consistency_type")

        if st.button("Submit Reset"):
            if new_replication_factor.strip().isdigit():
                command = f"reset_config {new_replication_factor} {new_consistency_type}"
                response = send_command(command)

                st.markdown(
                    f"""
                    <div style='display: flex; justify-content: center;'>
                        <div style='width:60%; padding:10px; margin-bottom:15px; border-radius:5px; background-color:#d4edda; 
                                    color:#155724; font-weight:bold; font-size:17px; text-align: center;'>
                            Reset Successful: Replication Factor = {new_replication_factor}, Consistency = {new_consistency_type} <br>
                            <strong>Response:</strong> {response}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            else:
                st.error("Please enter a valid numeric replication factor.")
# ---- OVERLAY PAGE ----
elif selected == "Overlay":
    st.markdown("<h2 style='text-align: center;'>Overlay</h2>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 4, 1])  # Center column takes more space

    with col2:
        # REFRESH BUTTON
        col24, col25, col26 = st.columns([4, 1, 4])
        with col25:
            if st.button("Refresh"):
                st.rerun()

        visualize_chord_ring()  # Display the network graph

        st.markdown("### Join the Network")

        col27, col28, col29, col299 = st.columns([4, 2, 2, 2])
        # JOIN SECTION
        with col27:
            join_ip = st.text_input("Enter IP Address:", key="join_ip")
            is_bootstrap = st.checkbox("Bootstrap", key="is_bootstrap", value=False)
        with col28:
            join_port = st.text_input("Enter Port:", key="join_port")
        with col29:
            join_vm = st.text_input("Enter VM Number:", key="join_vm")
        join_replication_factor, join_consistency = None, None
        if is_bootstrap:
            col30, col31 = st.columns([2, 2])
            with col30:
                join_replication_factor = st.text_input("Replication Factor:", value="3", key="join_replication_factor")
            with col31:
                join_consistency = st.selectbox("Consistency:", ["chain", "eventual"], key="join_consistency")

        with col299:
            st.markdown(
                """
                <style>
                div.stButton > button {
                    height: 36px !important; /* Match text_input height */
                    margin-top: 12.5px !important; /* Move button slightly down */
                    width: 100%;
                }
                </style>
                """,
                unsafe_allow_html=True
            )
            if st.button("Join"):
                if join_ip.strip() and join_port.strip() and join_vm.strip():
                    response = ssh_run_node(vm_number=join_vm,
                                            ip=join_ip,
                                            port=join_port,
                                            bootstrap_ip=BOOTSTRAP_IP,
                                            bootstrap_port=BOOTSTRAP_PORT,
                                            is_bootstrap=is_bootstrap,
                                            replication_factor=join_replication_factor,
                                            consistency=join_consistency)
                    st.success(f"{response}")
                    time.sleep(0.2)
                    if "Success" in response:
                        if is_bootstrap:
                            BOOTSTRAP_IP = join_ip
                            BOOTSTRAP_PORT = join_port
                        st.rerun()
                else:
                    st.error("Please enter IP address, Port, and VM Number.")

        # DEPART SECTION
        st.markdown("### Depart the Network")
        col210, col211 = st.columns([4, 1])
        with col210:
            depart_node_id = st.text_input("Enter Node ID to Depart:", key="depart_node_id")

        with col211:
            st.markdown(
                """
                <style>
                div.stButton > button {
                    height: 36px !important; /* Match text_input height */
                    margin-top: 12.5px !important; /* Move button slightly down */
                    width: 100%;
                }
                </style>
                """,
                unsafe_allow_html=True
            )
            if st.button("Depart"):
                if depart_node_id.strip():
                    # Search for the full node ID based on the last 4 digits
                    matched_nodes = [
                        full_id for full_id in st.session_state["node_info"]
                        if full_id.endswith(depart_node_id)
                    ]

                    if not matched_nodes:
                        st.error("No matching node found with the given last 4 digits.")
                    elif len(matched_nodes) > 1:
                        st.error("Multiple nodes found with the same last 4 digits. Please specify more uniquely.")
                    else:
                        full_node_id = matched_nodes[0]
                        node_info = st.session_state["node_info"].get(full_node_id)

                        if node_info:
                            vm_number = node_info["vm"]
                            ip = node_info["ip"]
                            port = node_info["port"]
                            vm_alias = f"team_2-vm{vm_number}"
                            kill_command = f"kill -2 $(lsof -t -i :{port})"

                            if vm_number == -1:
                                subprocess.run(kill_command, shell=True)
                                st.success(f"Node {depart_node_id} departed successfully from local machine.")
                            else:
                                # SSH into the correct VM and kill the process
                                ssh_command = f"ssh {vm_alias} '{kill_command}'"
                                subprocess.run(ssh_command, shell=True)
                                st.success(f"Node {depart_node_id} departed successfully from {vm_alias}.")

                            st.rerun()
                        else:
                            st.error("Node ID not found in stored overlay.")
                else:
                    st.error("Please enter a valid Node ID.")

        # Fetch Data from a Node
        st.markdown("### Fetch Data from a Node")
        selected_node = st.text_input("Enter Node ID to Fetch Data:", key="fetch_node_id")

        if st.button("Fetch Node Data"):
            if selected_node.strip():
                node_data = fetch_data_from_node(selected_node)

                if "error" in node_data:
                    st.error(node_data["error"])
                else:
                    if not node_data.get("data"):
                        st.info("This node has no stored data.")
                    else:
                        formatted_data = [{"Key": k, "Value": v} for k, v in node_data["data"].items()]
                        st.table(formatted_data)
            else:
                st.warning("Please enter a valid Node ID.")
# ---- EXPERIMENTS ----
if selected == "Experiments":
    st.markdown("<h2 style='text-align: center;'>Experiments</h2>", unsafe_allow_html=True)

    experiment_type = st.selectbox("Select Experiment Type", ["Write Throughput", "Read Throughput", "Freshness"])


    def process_insert_directory(directory):
        """Process batch insert from directory files."""
        if not os.path.exists(directory) or not os.path.isdir(directory):
            return False, 0, None, 0

        config_response = send_command("get_network_config").strip()
        if ":" in config_response:
            replication_factor, consistency = config_response.split(":")
            network_config = f"Replication Factor: {replication_factor}, Consistency: {consistency}"
        else:
            network_config = "Failed to fetch network configuration"

        insert_files = sorted(f for f in os.listdir(directory) if f.startswith("insert_") and f.endswith(".txt"))
        if not insert_files:
            return False, 0, network_config, 0

        start_time = time.time()
        key_counter = 0

        for filename in insert_files:
            filepath = os.path.join(directory, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as file:
                    value = filename.split('_')[1]  # Extract number part
                    keys = [line.strip() for line in file if line.strip()]

                    for key in keys:
                        key_counter += 1
                        command = f"insert \"{key}\" {value}"
                        response = send_command(command)
            except Exception:
                return False, 0, network_config, 0

        elapsed_time = time.time() - start_time
        return True, elapsed_time, network_config, key_counter


    def process_query_directory(directory):
        """Process batch queries from directory files."""
        if not os.path.exists(directory) or not os.path.isdir(directory):
            return False, 0, None, 0

        config_response = send_command("get_network_config").strip()
        if ":" in config_response:
            replication_factor, consistency = config_response.split(":")
            network_config = f"Replication Factor: {replication_factor}, Consistency: {consistency}"
        else:
            network_config = "Failed to fetch network configuration"

        query_files = sorted(f for f in os.listdir(directory) if f.startswith("query_") and f.endswith(".txt"))
        if not query_files:
            return False, 0, network_config, 0

        start_time = time.time()
        key_counter = 0

        for filename in query_files:
            filepath = os.path.join(directory, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as file:
                    keys = [line.strip() for line in file if line.strip()]

                    for key in keys:
                        key_counter += 1
                        command = f"query \"{key}\""
                        response = send_command(command)
            except Exception:
                return False, 0, network_config, 0

        elapsed_time = time.time() - start_time
        return True, elapsed_time, network_config, key_counter


    def process_request_directory(request_directory):
        """Process insert and query requests from all files in a directory."""
        if not os.path.exists(request_directory) or not os.path.isdir(request_directory):
            return False, "Directory not found", None

        config_response = send_command("get_network_config").strip()
        replication_factor, consistency = None, None
        if ":" in config_response:
            replication_factor, consistency = config_response.split(":")
            network_config = f"Replication Factor: {replication_factor}, Consistency: {consistency}"
        else:
            network_config = "Failed to fetch network configuration"

        responses = []
        request_files = sorted(
            f for f in os.listdir(request_directory) if f.startswith("requests_") and f.endswith(".txt"))

        for request_file in request_files:
            file_path = os.path.join(request_directory, request_file)
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    parts = [p.strip() for p in line.strip().split(",")]
                    if not parts:
                        continue

                    command_type = parts[0].lower()
                    if command_type == "insert" and len(parts) == 3:
                        key, value = parts[1], parts[2]
                        command = f"insert \"{key}\" {value}"
                        send_command(command)
                    elif command_type == "query" and len(parts) == 2:
                        key = parts[1]
                        command = f"query \"{key}\""
                        response = send_command(command)
                        responses.append((key, response))
        return True, responses, network_config


    def reset_config(replication_factor, consistency_type):
        """Reset the network configuration."""
        if not replication_factor.isdigit():
            return "Error: Replication factor must be a number."
        command = f"reset_config {replication_factor} {consistency_type}"
        response = send_command(command)

        try:
            response_data = json.loads(response)
            if isinstance(response_data, dict) and all(v == "ACK" for v in response_data.values()):
                return "OK"
        except json.JSONDecodeError:
            return f"Error: {response}"

        return f"Error: {response}"


    if experiment_type == "Write Throughput":
        insert_directory = st.text_input("Enter directory path for batch insert", "insert")
        if st.button("Run Write Throughput Experiment"):
            st.write("Running Write Throughput Experiment...")

            try:
                settings = [
                    ("1", "chain"), ("3", "chain"), ("5", "chain"),
                    ("1", "eventual"), ("3", "eventual"), ("5", "eventual")
                ]
                results = []
                chain_throughput = []
                eventual_throughput = []
                x_labels = []

                with tqdm(total=len(settings), desc="Running Experiment", unit="config") as pbar:
                    progress_bar = st.progress(0)  # Initialize progress bar
                    total_steps = len(settings)

                    for i, (repl_factor, consistency) in enumerate(settings):
                        progress_bar.progress(int((i / total_steps) * 100))  # Update progress

                    # for repl_factor, consistency in settings:
                        reset_status = reset_config(repl_factor, consistency)
                        if reset_status != "OK":
                            progress_bar.progress(100)
                            raise Exception(f"Resetting configuration failed: {reset_status}")
                        tqdm.write(
                            f"\nSetting Replication Factor={repl_factor}, Consistency={consistency}: {reset_status}")

                        success, elapsed_time, network_config, key_counter = process_insert_directory(insert_directory)
                        if not success:
                            progress_bar.progress(100)
                            raise Exception(f"Processing insert directory: {insert_directory} failed")
                        else:
                            throughput = key_counter / elapsed_time
                            results.append([repl_factor, consistency, key_counter, f"{elapsed_time:.2f} sec"])
                            if consistency == "chain":
                                chain_throughput.append(throughput)
                            else:
                                eventual_throughput.append(throughput)
                            x_labels.append(f"{repl_factor}")

                        pbar.update(1)
                        progress_bar.progress(int(((i + 1) / total_steps) * 100))  # Update progress
                        tqdm.write("Let the Conchord rest for 1 second.")
                        time.sleep(1)
                    progress_bar.progress(100)

                df = pd.DataFrame(results,
                                  columns=["Replication Factor", "Consistency", "Keys Inserted", "Time Taken (s)"])
                df["Time Taken (s)"] = df["Time Taken (s)"].str.replace(" sec", "", regex=False).astype(float)
                df["Write Throughput (Keys/sec)"] = df["Keys Inserted"] / df["Time Taken (s)"]
                df = df.drop(columns=["Keys Inserted"])
                st.table(df)
                st.success("Write Throughput Experiment Completed")

                col1, col2, col3 = st.columns([1,3,1])
                with col2:
                    fig, ax = plt.subplots(figsize=(8, 5))

                    chain_color = "#80C7E0"  # Muted blue
                    eventual_color = "#B490C0"  # Soft purple

                    ax.plot(x_labels[:3], chain_throughput, marker='o', linestyle='-', color=chain_color,
                            markersize=7, linewidth=2, alpha=0.8, label="Chain")
                    ax.plot(x_labels[3:], eventual_throughput, marker='s', linestyle='-', color=eventual_color,
                            markersize=7, linewidth=2, alpha=0.8, label="Eventual")
                    ax.set_xlabel("Replication Factor", fontsize=11, fontweight='medium',
                                  color="#E0E0E0")  # Soft gray-white
                    ax.set_ylabel("Throughput (Keys/sec)", fontsize=11, fontweight='medium', color="#E0E0E0")
                    ax.set_title("Write Throughput: Chain vs. Eventual Consistency", fontsize=13, fontweight='medium',
                                 color="#F5F5F5")
                    ax.grid(True, linestyle="--", alpha=0.3, color="gray")
                    ax.spines["left"].set_color("#A0A0A0")  # Light gray for softer contrast
                    ax.spines["bottom"].set_color("#A0A0A0")
                    ax.spines["right"].set_color("none")
                    ax.spines["top"].set_color("none")

                    ax.legend(facecolor="#222831", edgecolor="#444", fontsize=10, loc="upper right", framealpha=0.6)
                    st.pyplot(fig)


            except Exception as e:
                st.error(f"Error : {e}")

    elif experiment_type == "Read Throughput":
        col1, col2 = st.columns([1, 1])
        with col1:
            insert_directory = st.text_input("Enter directory path for batch insert", "insert")
        with col2:
            query_directory = st.text_input("Enter directory path for batch queries", "queries")
        if st.button("Run Read Throughput Experiment"):
            st.write("Running Read Throughput Experiment...")

            try:
                settings = [
                    ("1", "chain"), ("3", "chain"), ("5", "chain"),
                    ("1", "eventual"), ("3", "eventual"), ("5", "eventual")
                ]
                results = []
                chain_throughput = []
                eventual_throughput = []
                x_labels = []

                with tqdm(total=len(settings), desc="Running Experiment", unit="config") as pbar:
                    progress_bar = st.progress(0)  # Initialize progress bar
                    total_steps = len(settings)

                    for i, (repl_factor, consistency) in enumerate(settings):
                        progress_bar.progress(int((i / total_steps) * 100))  # Update progress

                    # for repl_factor, consistency in settings:
                        reset_status = reset_config(repl_factor, consistency)
                        if reset_status != "OK":
                            progress_bar.progress(100)
                            raise Exception(f"Resetting configuration failed: {reset_status}")
                        tqdm.write(
                            f"\nSetting Replication Factor={repl_factor}, Consistency={consistency}: {reset_status}")

                        success, elapsed_time, network_config, key_counter = process_insert_directory(insert_directory)
                        if not success:
                            progress_bar.progress(100)
                            raise Exception(f"Processing insert directory: {insert_directory} failed")

                        success, elapsed_time, network_config, key_counter = process_query_directory(query_directory)

                        if not success:
                            progress_bar.progress(100)
                            raise Exception(f"Processing insert directory: {insert_directory} failed")
                        else:
                            throughput = key_counter / elapsed_time
                            results.append([repl_factor, consistency, key_counter, f"{elapsed_time:.2f} sec"])
                            if consistency == "chain":
                                chain_throughput.append(throughput)
                            else:
                                eventual_throughput.append(throughput)
                            x_labels.append(f"{repl_factor}")

                        pbar.update(1)
                        progress_bar.progress(int(((i + 1) / total_steps) * 100))  # Update progress
                        tqdm.write("Let the Conchord rest for 1 second.")
                        time.sleep(1)
                    progress_bar.progress(100)

                df = pd.DataFrame(results,
                                  columns=["Replication Factor", "Consistency", "Keys Queried", "Time Taken (s)"])
                df["Time Taken (s)"] = df["Time Taken (s)"].str.replace(" sec", "", regex=False).astype(float)
                df["Read Throughput (Queries/sec)"] = df["Keys Queried"] / df["Time Taken (s)"]
                df = df.drop(columns=["Keys Queried"])
                st.table(df)
                st.success("Read Throughput Experiment Completed")


                col1, col2, col3 = st.columns([1,3,1])
                with col2:
                    fig, ax = plt.subplots(figsize=(8, 5))

                    chain_color = "#80C7E0"  # Muted blue
                    eventual_color = "#B490C0"  # Soft purple

                    ax.plot(x_labels[:3], chain_throughput, marker='o', linestyle='-', color=chain_color,
                            markersize=7, linewidth=2, alpha=0.8, label="Chain")
                    ax.plot(x_labels[3:], eventual_throughput, marker='s', linestyle='-', color=eventual_color,
                            markersize=7, linewidth=2, alpha=0.8, label="Eventual")
                    ax.set_xlabel("Replication Factor", fontsize=11, fontweight='medium',
                                  color="#E0E0E0")  # Soft gray-white
                    ax.set_ylabel("Read Throughput (Queries/sec)", fontsize=11, fontweight='medium', color="#E0E0E0")
                    ax.set_title("Read Throughput: Chain vs. Eventual Consistency", fontsize=13, fontweight='medium',
                                 color="#F5F5F5")
                    ax.grid(True, linestyle="--", alpha=0.3, color="gray")
                    ax.spines["left"].set_color("#A0A0A0")  # Light gray for softer contrast
                    ax.spines["bottom"].set_color("#A0A0A0")
                    ax.spines["right"].set_color("none")
                    ax.spines["top"].set_color("none")

                    ax.legend(facecolor="#222831", edgecolor="#444", fontsize=10, loc="upper left", framealpha=0.6)
                    st.pyplot(fig)

            except Exception as e:
                st.error(f"Error on batch insert: {e}")

    elif experiment_type == "Freshness":
        request_directory = st.text_input("Enter request directory path", "requests")
        if st.button("Run Freshness Experiment"):
            st.write("Running Freshness Experiment...")
            try:
                settings = [("3", "chain"), ("3", "eventual")]

                chain_results = []
                eventual_results = []

                with tqdm(total=len(settings), desc="Running Freshness Experiment", unit="config") as pbar:
                    progress_bar = st.progress(0)  # Initialize progress bar
                    total_steps = len(settings)

                    for i, (repl_factor, consistency) in enumerate(settings):
                        progress_bar.progress(int((i / total_steps) * 100))  # Update progress

                    # for repl_factor, consistency in settings:
                        reset_status = reset_config(repl_factor, consistency)
                        if reset_status != "OK":
                            progress_bar.progress(100)
                            raise Exception(f"Resetting configuration failed: {reset_status}")
                        tqdm.write(
                            f"\nSetting Replication Factor={repl_factor}, Consistency={consistency}: {reset_status}")

                        success, responses, network_config = process_request_directory(request_directory)
                        if not success:
                            progress_bar.progress(100)
                            raise Exception(f"Processing directory: {request_directory} failed")

                        if consistency == "chain":
                            for key, value in responses:
                                chain_results.append(["chain", key, value])
                        else:
                            for key, value in responses:
                                eventual_results.append(["eventual", key, value])

                        pbar.update(1)
                        progress_bar.progress(int(((i + 1) / total_steps) * 100))  # Update progress
                        tqdm.write("Let the Conchord rest for 1 second.")
                        time.sleep(1)
                    progress_bar.progress(100)

                df_chain = pd.DataFrame(chain_results, columns=["Consistency", "Key", "Value"]).drop(
                    columns=["Consistency"])
                df_eventual = pd.DataFrame(eventual_results, columns=["Consistency", "Key", "Value"]).drop(
                    columns=["Consistency"])

                col1, col2 = st.columns([1,1])
                with col1:
                    df_chain_styled = df_chain.style.set_table_styles([
                        {'selector': 'thead th:nth-child(1)', 'props': [('width', '10%')]},  # Index
                        {'selector': 'thead th:nth-child(2)', 'props': [('width', '30%')]},  # Key
                        {'selector': 'thead th:nth-child(3)', 'props': [('width', '60%')]}  # Value
                    ])
                    st.markdown("<h3 style='text-align: center;'>Chain Consistency Results</h3>", unsafe_allow_html=True)
                    st.dataframe(df_chain_styled, use_container_width=True)
                with col2:
                    df_eventual_styled = df_eventual.style.set_table_styles([
                        {'selector': 'thead th:nth-child(1)', 'props': [('width', '10%')]},  # Index
                        {'selector': 'thead th:nth-child(2)', 'props': [('width', '30%')]},  # Key
                        {'selector': 'thead th:nth-child(3)', 'props': [('width', '60%')]}  # Value
                    ])
                    st.markdown("<h3 style='text-align: center;'>Eventual Consistency Results</h3>", unsafe_allow_html=True)
                    st.dataframe(df_eventual_styled, use_container_width=True)

                import difflib

                df_chain_reset = df_chain.reset_index()
                df_eventual_reset = df_eventual.reset_index()
                df_diff = df_chain_reset.copy()
                df_diff["Eventual"] = df_eventual_reset["Value"]
                df_diff_filtered = df_diff[df_diff["Value"] != df_diff["Eventual"]].drop(columns=["index"])
                df_diff_filtered = df_diff_filtered.rename(columns={"Value": "Chain"})

                def get_extra_words(chain_val, eventual_val):
                    chain_words = set(str(chain_val).split(", "))  # Convert to sets for comparison
                    eventual_words = set(str(eventual_val).split(", "))
                    extra_in_chain = ", ".join(chain_words - eventual_words) if chain_words - eventual_words else None
                    extra_in_eventual = ", ".join(
                        eventual_words - chain_words) if eventual_words - chain_words else None
                    return pd.Series([extra_in_chain, extra_in_eventual], index=["Extra in Chain", "Extra in Eventual"])
                df_extra = df_diff_filtered.apply(lambda row: get_extra_words(row["Chain"], row["Eventual"]), axis=1)
                df_final = df_diff_filtered.join(df_extra)
                df_final = df_final.dropna(axis=1, how="all")

                st.markdown("<h3 style='text-align: center;'>Diff</h3>", unsafe_allow_html=True)
                st.dataframe(df_final, use_container_width=True)

                st.success("Experiment Completed")
            except Exception as e:
                st.error(f"Error: {e}")

# ---- FOOTER ----
current_year = datetime.datetime.now().year
st.markdown(f"<div style='text-align: center; padding-top:20px;'>🎼 Conchord © {current_year}</div>", unsafe_allow_html=True)