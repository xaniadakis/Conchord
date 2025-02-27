# Chord Network Simulator - README

## CLI Mode
Run in CLI mode:
```sh
python3 chord_network.py --cli <num_nodes>
```

### Commands
| Command | Description | Example |
|---------|------------|---------|
| `join <ip> <port>` | Add a node | `join 127.0.0.1 5001` |
| `depart <short_id>` | Remove a node (last 4 digits of ID) | `depart 7821` |
| `overlay` | Show network graph | `overlay` |
| `exit` | Shut down | `exit` |

## GUI Mode
Run in GUI mode:
```sh
python3 chord_network.py --m <mode> -i <bootstrap_ip> -p <bootstrap_port> -n <num_nodes> -r <replication_factor> -c <consistency>
```
or:
```sh
python3 script.py --mode <mode> --bootstrap_ip <bootstrap_ip> --bootstrap_port <bootstrap_port> --num_nodes <num_nodes> --replication_factor <replication_factor> --consistency <consistency>
```


Example:
```sh
python3 chord_network.py --mode gui -i 127.0.0.1 -p 5000 -n 5 -r 2 -c eventual
```

### Features
- **Join Node**: Automatically assigns a port.
- **Depart Node**: Remove by Short ID (last 4 digits of Node ID).
- **Exit**: Clean shutdown, ensuring all nodes have departed.
- **Live Visualization**: Network graph automatically updates itself every time a node joins or departs.
  - **Node Labels**: `short_id | last2_port_digits`  
     _(e.g., Node ID: `135117988181535955003563159026686220824819050071` & Port: `5001` → Label: `0071|01`)_  
     We follow this format to keep visualization simple, yet informative.
  - **Clickable Nodes**: Click on a node in the visualization to see stored keys.
  - **Key Count Display**: Each node displays the number of stored keys near it.

## Client CLI
The `client.py` script allows interaction with the Chord network using commands like `insert`, `query`, and `delete`.

### Running the Client
1. **Start the Chord network first** using either CLI or GUI mode.
2. **Run the client script**:
   ```sh
   python3 client.py
   ```

### Bulk Insert from Files
- The client automatically processes `insert_*.txt` files from the `insert/` directory.
- After the client finishes populating the nodes, tap the **Refresh** button to display the latest node storage information.