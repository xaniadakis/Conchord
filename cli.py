import socket
import readline  # Enables history and arrow navigation


def send_command(command):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect(('127.0.0.1', 5000))
            client.sendall(command.encode())
            response = client.recv(1024).decode()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Error: {e}")


def validate_command(command):
    parts = command.split()
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


if __name__ == "__main__":
    print("Commands: insert <key> <value>, query <key>, delete <key>, help")

    # Enable persistent history (up/down arrows)
    readline.set_history_length(100)  # Store up to 100 commands

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
