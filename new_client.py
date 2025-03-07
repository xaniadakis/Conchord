import streamlit as st
from streamlit_option_menu import option_menu
import datetime
import socket
import matplotlib.pyplot as plt
import networkx as nx
import json

# ---- PAGE CONFIG ----
st.set_page_config(page_title="ConChord", page_icon="🎼", layout="wide")

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

# ---- FUNCTION: SEND COMMAND TO CHORD NETWORK ----
def send_command(command):
    """Send a command to the bootstrap node and return the response."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect(('127.0.0.1', 5000))  # Connect to bootstrap node
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
    node_colors = []
    text_colors = {}

    # Ensure node IDs are always strings
    nodes = {str(node_id): details for node_id, details in nodes.items()}

    # Store node positions for later use in button mapping
    node_positions = {}

    # Explicitly add all nodes first
    for node_id, details in nodes.items():
        node_id = str(node_id)  # Convert to string
        G.add_node(node_id)
        labels[node_id] = f"{node_id[-4:]} | {str(details.get('port', '??'))[-2:]}"  # Short ID | Port

        # Assign colors
        if details.get("is_bootstrap", False):
            node_colors.append("#BE3144")  # Bootstrap node (Red)
            text_colors[node_id] = "white"
        else:
            node_colors.append("lightblue")  # Normal nodes (Blue)
            text_colors[node_id] = "black"

    # Add edges
    for node_id, details in nodes.items():
        node_id = str(node_id)
        successor = details.get("successor")

        if successor is not None:
            successor = str(successor)  # Convert to string
            if successor in nodes:
                G.add_edge(node_id, successor)

    # Generate positions AFTER all nodes are added
    pos = nx.circular_layout(G)

    # Store positions for node click interaction
    node_positions = {node: pos[node] for node in G.nodes}

    # Draw nodes and edges
    nx.draw(G, pos, with_labels=False, node_size=800, node_color=node_colors, edge_color="gray",
            font_size=6, arrowsize=10, ax=ax)

    # Draw labels with specific text colors
    for node, (x, y) in pos.items():
        color = text_colors.get(node, "black")
        ax.text(x, y, labels.get(node, ""), fontsize=6, color=color,
                ha='center', va='center', bbox=dict(facecolor='none', edgecolor='none', pad=0))

    # Show key counts above nodes
    for node, (x, y) in pos.items():
        key_count = nodes.get(node, {}).get("key_count", 0)
        ax.text(x, y + 0.1, str(key_count), fontsize=6, color='white',
                ha='center', va='center', bbox=dict(facecolor='black', edgecolor='black',
                                                    boxstyle='round,pad=0.5', alpha=0.75))

    # Store node positions for interaction
    st.session_state["node_positions"] = node_positions

    st.pyplot(fig)

# ---- DISPLAY SELECTED NODE KEYS ----
def display_selected_node_keys():
    """Display keys of the selected node when clicked."""
    if "selected_node" in st.session_state and st.session_state["selected_node"]:
        selected_node = st.session_state["selected_node"]
        stored_keys = st.session_state.get("selected_keys", [])

        st.markdown(f"### Stored Keys in Node {selected_node[-4:]}")
        if stored_keys:
            st.text_area("Stored Keys", "\n".join(map(str, stored_keys)), height=200)
        else:
            st.info("No keys stored in this node.")
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
                st.success(f"Response: {response}")
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
                st.success(f"Response: {response}")
            else:
                st.error("Please enter a key.")

    # ---- DELETE ACTION ----
    elif st.session_state["action"] == "delete":
        st.markdown("<h3>Delete Data</h3>", unsafe_allow_html=True)

        delete_input = st.text_input("Enter Key to Delete:", key="delete_key")

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
    col1, col2, col3 = st.columns([1, 4, 1])  # Make the center column take more space

    with col2:
        col21, col22, col23 = st.columns([4, 1, 4])  # Make the center column take more space
        with col22:
            if st.button("Refresh"):
                st.rerun()
        visualize_chord_ring()  # This ensures it's constrained in the middle

        # User selects a node to fetch its data
        st.markdown("### Fetch Data from a Node")
        selected_node = st.text_input("Enter Node ID to Fetch Data:")

        if st.button("Fetch Node Data"):
            if selected_node.strip():
                node_data = fetch_data_from_node(selected_node)

                if "error" in node_data:
                    st.error(node_data["error"])  # Show error if retrieval failed
                else:
                    if not node_data.get("data"):  # If the node has no data
                        st.info("This node has no stored data.")
                    else:
                        # Convert data dictionary to a table
                        formatted_data = [{"Key": k, "Value": v} for k, v in node_data["data"].items()]
                        st.table(formatted_data)  # Display as a structured table
            else:
                st.warning("Please enter a valid Node ID.")

# ---- FOOTER ----
current_year = datetime.datetime.now().year
st.markdown(f"<div style='text-align: center; padding-top:20px;'>🎼 Conchord © {current_year}</div>", unsafe_allow_html=True)
