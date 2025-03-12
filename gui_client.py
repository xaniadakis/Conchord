import streamlit as st
from streamlit_option_menu import option_menu
import datetime
import socket
import matplotlib.pyplot as plt
import networkx as nx
import json

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
import subprocess


def ssh_run_node(vm_number, ip, port, bootstrap_ip, bootstrap_port):
    """SSH into the selected VM using the alias and run the node.py script."""
    vm_alias = f"team_2-vm{vm_number}"  # Use the configured alias

    # command = f"ssh {vm_alias} 'python3 ~/conchord/node.py --ip {ip} --port {port} --bootstrap_ip {bootstrap_ip} --bootstrap_port {bootstrap_port}'"
    command = f"ssh {vm_alias} 'python3 ~/conchord/node.py --ip {ip} --port {port} --bootstrap_ip 10.0.9.91 --bootstrap_port 5000'"

    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            print(f"OK: {result.stdout}")
            return f"Success: {result.stdout}"
        else:
            print(f"ERR: {result.stderr}")
            return f"Error: {result.stderr}"

    except Exception as e:
        return f"SSH Connection Error: {e}"

# ---- FUNCTION: SEND COMMAND TO CHORD NETWORK ----
def send_command(command):
    """Send a command to the bootstrap node and return the response."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect(('3.67.245.126', 5000))  # Connect to bootstrap node
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
        st.error(f"Failed to fetch network: {nodes['error']}")
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

# ---- SIDEBAR NAVIGATION ----
with st.sidebar:
    selected = option_menu(
        menu_title="Navigation",
        options=["Operations", "Overlay"],
        icons=["database", "globe"],
        menu_icon="cast",
        default_index=0,
    )

# ---- DATABASE OPERATIONS PAGE ----
if selected == "Operations":
    st.markdown("<h2 style='text-align: center;'>Operations</h2>", unsafe_allow_html=True)

    st.subheader("Choose an action:")
    col1, col2, col3, col4 = st.columns(4)

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
        with col28:
            join_port = st.text_input("Enter Port:", key="join_port")
        with col29:
            join_vm = st.text_input("Enter VM Number:", key="join_vm")
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
                    response = ssh_run_node(join_vm, join_ip, join_port, "127.0.0.1",
                                            "5000")  # Bootstrap IP and Port are fixed
                    st.success(f"Response: {response}")
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
                    command = f'depart {depart_node_id}'
                    response = send_command(command)
                    st.warning(f"Response: {response}")
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

# ---- FOOTER ----
current_year = datetime.datetime.now().year
st.markdown(f"<div style='text-align: center; padding-top:20px;'>🎼 Conchord © {current_year}</div>", unsafe_allow_html=True)
