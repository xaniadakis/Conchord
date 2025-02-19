# Chord Network Simulator - README

## CLI Mode
Run in CLI mode:
```sh
python3 chord_network.py --cli
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
python3 chord_network.py --gui
```

### Features
- **Join Node**: Automatically assigns a port.
- **Depart Node**: Remove by Short ID (last 4 digits of Node ID).
- **Exit**: Clean shutdown, ensuring all nodes have departed.
- **Live Visualization**: Network graph automatically updates itself every time a node joins or departs.
  - **Node Labels**: `short_id | last2_port_digits`  
     _(e.g., Node ID: `135117988181535955003563159026686220824819050071` & Port: `5001` → Label: `0071|01`)_  
     We follow this format to keep visualization simple, yet informative.