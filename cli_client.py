import argparse
import json
import socket
from tqdm import tqdm
import readline
from colorama import Fore, Style, init
import os

init(autoreset=True)

server_ip = None
server_port = None

def send_command(command, timeout=1):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.settimeout(timeout)
            client.connect((server_ip, server_port))
            client.sendall(command.encode())

            response = []
            while True:
                # read in chunks
                chunk = client.recv(4096).decode()
                # stop when no more data arrives
                if not chunk:
                    break
                response.append(chunk)

            return ''.join(response)

    except socket.timeout:
        print(f"{Fore.YELLOW}Error: Connection timed out after {timeout} seconds.{Style.RESET_ALL}")
    except ConnectionRefusedError:
        print(f"{Fore.RED}Unable to connect to the server. \n"
              f"Make sure the bootstrap node is up on {server_ip}:{server_port} and restart!{Style.RESET_ALL}")
        os._exit(1)
    except Exception as e:
        print(f"Error: {e}")
        return None

def fetch_nodes():
    try:
        response = send_command("overlay")  # Send overlay request
        if not response.strip():
            return {"error": "Empty response from network"}

        nodes = json.loads(response)
        return nodes

    except json.JSONDecodeError as x:
        return {"error": f"Invalid JSON format from server {x}"}
    except Exception as e:
        return {"error": str(e)}


def insert_data(key, value):
    if key.strip() and value.strip():
        command = f'insert "{key}" {value}'
        response = send_command(command)
        print(f"Response: {response}")
    else:
        print("Please enter both a key and a value.")

def query_data(key):
    if key.strip():
        command = f'query "{key}"'
        response = send_command(command)
        print(f"Response: {response}")
    else:
        print("Please enter a key.")


def delete_data(key):
    if key.strip():
        command = f'delete "{key}"'
        response = send_command(command)
        print(f"Response: {response}")
    else:
        print("Please enter a key.")


def fetch_overlay():
    nodes = fetch_nodes()

    if "error" in nodes:
        print(f"Error: {nodes['error']}")
        return

    for node_id, details in sorted(nodes.items(), key=lambda item: item[0], reverse=False):
        successor = details.get("successor")
        predecessor = details.get("predecessor")
        key_count = details.get("key_count")
        print(f"Predecessor: {str(predecessor)[-4:]} <= Node: {str(node_id)[-4:]} (Key Count: {key_count}) => Successor: {str(successor)[-4:]}")


def validate_command(command):
    parts = command.split()
    if len(parts) == 0:
        return False, "Command cannot be empty."

    if parts[0].lower() not in ["insert", "query", "delete", "overlay", "help"]:
        return False, f"Unknown command: {parts[0]}"

    if parts[0].lower() == "insert" and len(parts) != 3:
        return False, "Insert command requires two arguments: <key> <value>"

    if parts[0].lower() == "query" and len(parts) != 2:
        return False, "Query command requires one argument: <key>"

    if parts[0].lower() == "delete" and len(parts) != 2:
        return False, "Delete command requires one argument: <key>"

    return True, ""


def process_command(command):
    parts = command.split()

    if parts[0].lower() == "insert":
        insert_data(parts[1], parts[2])
    elif parts[0].lower() == "query":
        query_data(parts[1])
    elif parts[0].lower() == "delete":
        delete_data(parts[1])
    elif parts[0].lower() == "overlay":
        fetch_overlay()
    elif parts[0].lower() == "help":
        print("Commands: insert <key> <value>, query <key>, delete <key>, help")


def process_insert_directory(directory):
    if not os.path.exists(directory) or not os.path.isdir(directory):
        print(f"Error: Directory '{directory}' does not exist.")
        return

    insert_files = sorted(f for f in os.listdir(directory) if f.startswith("insert_") and f.endswith(".txt"))

    total_files = len(insert_files)
    if total_files == 0:
        print("No insert files found.")
        return

    print(f"Processing {total_files} insert files...\n")

    for filename in tqdm(insert_files, desc="Processing Files", unit="file", ncols=80):
        filepath = os.path.join(directory, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                value = filename.split('_')[1]
                keys = [line.strip() for line in file if line.strip()]

                for key in tqdm(keys, desc=f"Inserting keys from {filename}", unit="key", leave=False, ncols=80):
                    command = f"insert \"{key}\" {value}"
                    response = send_command(command)

                    if not response or "error" in response.lower() or "400" in response or "failed" in response:
                        print(f"\nError while inserting {key}: {response}")
                        print("Batch insert failed! Exiting...")
                        os._exit(1)

        except Exception as e:
            print(f"Error processing {filename}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chord Network CLI Client with Batch Insert")
    parser.add_argument("--batch-insert", action="store_true", default=False, help="Whether to perform batch insert operation from directory.")
    parser.add_argument("--server-ip", type=str, default="127.0.0.1", help="The IP address of the bootstrap node (default: 127.0.0.1)")
    parser.add_argument("--server-port", type=int, default=5000, help="The port of the bootstrap node (default: 5000)")

    args = parser.parse_args()

    server_ip = args.server_ip
    server_port = args.server_port

    insert_dir = "insert"
    if args.batch_insert:
        process_insert_directory(insert_dir)

    readline.set_history_length(100)
    print("Commands: insert <key> <value>, query <key>, delete <key>, help")

    while True:
        try:
            command = input("> ").strip()

            if not command:
                print("Command cannot be empty.")
                continue

            valid, message = validate_command(command)
            if not valid:
                print(f"Error: {message}")
                continue

            process_command(command)

        except KeyboardInterrupt:
            print("\nExiting CLI. Goodbye!")
            break