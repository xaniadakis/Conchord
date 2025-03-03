import os
import socket
import readline  # Enables history and arrow navigation
import time
from tqdm import tqdm  # ✅ Progress bar
from utils import custom_split

def send_command(command):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect(('127.0.0.1', 5000))
            client.sendall(command.encode())

            response = []
            while True:
                # read in chunks
                chunk = client.recv(4096).decode()
                # stop when no more data arrives
                if not chunk:
                    break
                response.append(chunk)
            print(f"Response: {"".join(response)}")

    except Exception as e:
        print(f"Error: {e}")

def validate_command(command):
    # parts = command.split()
    parts = custom_split(command)
    commands_usage = {
        "insert": "Usage: insert <key> <value>",
        "query": "Usage: query <key>",
        "delete": "Usage: delete <key>",
        "help": "Usage: help"
    }

    if len(parts) == 0:
        return False, "Command cannot be empty."

    action = parts[0].lower()
    if action not in commands_usage:
        return False, "Unknown command. Type 'help' for available commands."

    if (action == "insert" and len(parts) != 3) or \
            (action in ["query", "delete"] and len(parts) != 2) or \
            (action == "help" and len(parts) != 1):
        return False, commands_usage[action]

    return True, ""

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

    # ✅ Iterate over files with progress bar
    for filename in tqdm(insert_files, desc="Processing Files", unit="file", ncols=80):
        filepath = os.path.join(directory, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                value = filename.split('_')[1]  # Extract number part
                keys = [line.strip() for line in file if line.strip()]

                # ✅ Use tqdm to show progress per file
                for key in tqdm(keys, desc=f"  Inserting keys from {filename}", unit="key", leave=False, ncols=80):
                    command = f"insert \"{key}\" {value}"
                    send_command(command)

        except Exception as e:
            print(f"Error processing {filename}: {e}")

if __name__ == "__main__":
    print("Commands: insert <key> <value>, query <key>, delete <key>, help")

    # Enable persistent history (up/down arrows)
    readline.set_history_length(100)  # Store up to 100 commands

    insert_dir = "insert"  # Adjust this if needed
    process_insert_directory(insert_dir)

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

            if command.lower() == "help":
                print("Commands: insert <key> <value>, query <key>, delete <key>, help")
            else:
                send_command(command)

        except KeyboardInterrupt:
            print("\nExiting CLI. Goodbye!")
            break
