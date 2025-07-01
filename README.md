# ConChord

## Overview

ConChord is a distributed, peer-to-peer key-value store based on the Chord DHT protocol, designed to explore the 
trade-offs between consistency and performance. It features configurable replication, two consistency models 
(linearizability and eventual), and robust client interaction via both CLI and a real-time web-based GUI Streamlit app.

Deployed on a multi-node AWS cloud infrastructure, ConChord supports live node join/departure, decentralized lookups
and flexible topology management. 

[//]: # (This project implements a **Chord Distributed Hash Table &#40;DHT&#41;** in Python, supporting **key-value storage, node join/departure, replication, and consistency mechanisms**. It is designed to be used in a distributed network of nodes where each node is responsible for a portion of the keyspace.)

## Features
- **Decentralized peer-to-peer network** based on the Chord protocol.
- **Efficient key lookup** via consistent hashing.
- **Replication Factor**: Configurable redundancy to enhance fault tolerance.
- **Consistency Models**: Supports both *Chain* (strong consistency) and *Eventual* consistency.
- **Graceful Node Departure**: Ensures key redistribution when a node leaves.
- **Bootstrap Node**: Acts as the entry point for new nodes to join the network.
- **Overlay Querying**: Allows visualization of the network structure.
- **Data Insertion, Querying, and Deletion** commands for client interaction.
- **CLI and GUI Clients**: Interact with the network via command-line or a web-based interface using Streamlit.

## Experimental Highlights

- Benchmarked **write and read throughput** under various replication factors
- Compared data **freshness** between strong and eventual consistency
- Achieved >1700 reads/sec with 5 replicas under eventual consistency

---

## Running the Chord Network

### Start a Bootstrap Node
A **bootstrap node** is required to start the Chord network. It is responsible for initial configurations like replication factor and consistency.

```sh
python3 node.py --ip <bootstrap_ip> --port <bootstrap_port> --bootstrap \
                 --replication_factor <factor> --consistency <chain/eventual>
```

Example:
```sh
python3 node.py --ip 127.0.0.1 --port 5000 --bootstrap --replication_factor 3 --consistency chain
```

### Join a New Node
New nodes must connect to the bootstrap node to join the network.

```sh
python3 node.py --ip <node_ip> --port <node_port> --bootstrap_ip <bootstrap_ip> --bootstrap_port <bootstrap_port>
```

Example:
```sh
python3 node.py --ip 127.0.0.1 --port 5001 --bootstrap_ip 127.0.0.1 --bootstrap_port 5000
```

### Node Departure
Nodes can gracefully exit the network, transferring their data to their successor.

```sh
CTRL+C  # Gracefully exits
```

OR send a departure request manually:

```sh
echo "depart" | nc <node_ip> <node_port>
```

---

## Client Interaction
A **CLI client** and a **GUI client** are provided to interact with the Chord network.

### Running the CLI Client
```sh
python3 cli_client.py --server-ip <bootstrap_ip> --server-port <bootstrap_port>
```

### Running the GUI Client
The GUI client provides an interactive visualization of the Chord network using Streamlit.

#### Steps to Run the GUI Client:
1. Connect to the Bastion:
   ```sh
   ./connect_bastion.sh
   ```
2. Navigate to the project directory:
   ```sh
   cd ~/conchord
   ```
3. Activate the virtual environment:
   ```sh
   source .venv/bin/activate
   ```
4. Install required dependencies:
   ```sh
   pip install colorama tqdm streamlit networkx pandas matplotlib
   ```
5. Run the GUI client using Streamlit:
   ```sh
   sudo $(which streamlit) run gui_client.py --server.port 80
   ```

### CLI Client Commands
| Command | Description | Example |
|---------|------------|---------|
| `insert <key> <value>` | Store a key-value pair | `insert name Alice` |
| `query <key>` | Retrieve a value by key | `query name` |
| `delete <key>` | Remove a key-value pair | `delete name` |
| `overlay` | Display network topology | `overlay` |
| `exit` | Close client connection | `exit` |

### GUI Client Features
- **Live Visualization**: See the Chord ring and node connections.
- **Insert, Query, and Delete Operations**: Easily perform data operations.
- **Batch Insert**: Load key-value pairs from a directory.
- **Network Reset**: Change replication factor and consistency settings dynamically.

---

## Configuration Options
| Option | Description |
|--------|-------------|
| `--replication_factor` | Number of times each key is replicated across nodes |
| `--consistency` | Consistency model: `chain` (strong) or `eventual` (weak) |
| `--bootstrap` | Marks a node as the bootstrap node |
| `--bootstrap_ip` | IP address of the bootstrap node |
| `--bootstrap_port` | Port of the bootstrap node |

---
## Workflow

### Deploy a Chord Network (Automated Scripts)

```sh
./update_vm_node.sh  # Update the VMs with the current latest code from localhost
./run_nodes.sh 2 3 chain  # Start 2 nodes per VM, with replication factor 3 and chain consistency
```

### Insert & Query Keys via CLI Client

```sh
python3 cli_client.py --server-ip 127.0.0.1 --server-port 5000
> insert song1 value1
> insert song2 value2
> query song1
value1
> query song2
value2
> delete song1
> query song1
Key not found
```

### Stop all nodes

```sh
./kill_nodes.sh
```
---

## Contributors

- [Evangelos Chaniadakis](https://github.com/xaniadakis)
- [Tasos Kanellos](https://github.com/taskanell)
- [Dimitra Kallini](https://github.com/dimkallini)
