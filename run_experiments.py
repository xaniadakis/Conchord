import socket
import json
import time
import pandas as pd
from tqdm import tqdm
from tabulate import tabulate
import os

def save_results_to_csv(df, filename):
    df.to_csv(filename, index=False)
    print(f"Results saved to {filename}")

def send_command(command, host='127.0.0.1', port=5000):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((host, port))
            client.sendall(command.encode())
            response = []
            while True:
                chunk = client.recv(4096).decode()
                if not chunk:
                    break
                response.append(chunk)
            return "".join(response)
    except Exception as e:
        return f"Error: {e}"


def process_insert_directory(directory):
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
                value = filename.split('_')[1]
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
    request_files = sorted(f for f in os.listdir(request_directory) if f.startswith("requests_") and f.endswith(".txt"))

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
            # if consistency == "chain":
            #     input("\nPress Enter to carry on with next requests file...")
    return True, responses, network_config

def run_freshness_experiment(request_directory):
    settings = [("3", "chain"), ("3", "eventual")]
    chain_results = []
    eventual_results = []

    with tqdm(total=len(settings), desc="Running Freshness Experiment", unit="config") as pbar:
        for repl_factor, consistency in settings:
            reset_status = reset_config(repl_factor, consistency)
            tqdm.write(f"\nSetting Replication Factor={repl_factor}, Consistency={consistency}: {reset_status}")

            success, responses, network_config = process_request_directory(request_directory)
            if not success:
                print(f"Failed to process requests from {request_directory}")
                os._exit(1)

            if consistency == "chain":
                for key, value in responses:
                    chain_results.append(["chain", key, value])
            else:
                for key, value in responses:
                    eventual_results.append(["eventual", key, value])

            pbar.update(1)
            tqdm.write("Let the Conchord rest for 1 second.")

    df_chain = pd.DataFrame(chain_results, columns=["Consistency", "Key", "Value"]).drop(columns=["Consistency"])
    df_eventual = pd.DataFrame(eventual_results, columns=["Consistency", "Key", "Value"]).drop(columns=["Consistency"])

    save_results_to_csv(df_chain, "chain_freshness_experiment.csv")
    save_results_to_csv(df_eventual, "eventual_freshness_experiment.csv")

    input("\nSaved results to freshness_chain.csv and freshness_eventual.csv\nPress Enter to return to the menu...")

def reset_config(replication_factor, consistency_type):
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

def run_insert_experiment(directory):
    try:
        settings = [
            ("1", "chain"), ("3", "chain"), ("5", "chain"),
            ("1", "eventual"), ("3", "eventual"), ("5", "eventual")
        ]
        results = []

        with tqdm(total=len(settings), desc="Running Experiment", unit="config") as pbar:
            for repl_factor, consistency in settings:
                reset_status = reset_config(repl_factor, consistency)
                tqdm.write(f"\nSetting Replication Factor={repl_factor}, Consistency={consistency}: {reset_status}")

                success, elapsed_time, network_config, key_counter = process_insert_directory(directory)

                if success:
                    results.append([repl_factor, consistency, key_counter, f"{elapsed_time:.2f} sec"])
                else:
                    results.append([repl_factor, consistency, key_counter, "Failed"])

                pbar.update(1)
                tqdm.write("Let the Conchord rest for 1 second.")
                time.sleep(1)

        df = pd.DataFrame(results, columns=["Replication Factor", "Consistency", "Keys Inserted", "Time Taken (s)"])
        df["Time Taken (s)"] = df["Time Taken (s)"].str.replace(" sec", "", regex=False).astype(float)
        df["Throughput (Keys/sec)"] = df["Keys Inserted"] / df["Time Taken (s)"]
        df = df.drop(columns=["Keys Inserted"])
        print("\nExperiment Results:")
        print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
        save_results_to_csv(df, "write_throughput_experiment.csv")
        input("\nPress Enter to return to the menu...")
    except Exception as e:
        print(f"Error on batch insert: {e}")
        os._exit(1)

def run_query_experiment(insert_directory, queries_directory):
    try:
        settings = [
            ("1", "chain"), ("3", "chain"), ("5", "chain"),
            ("1", "eventual"), ("3", "eventual"), ("5", "eventual")
        ]
        results = []

        with tqdm(total=len(settings), desc="Running Query Experiment", unit="config") as pbar:
            for repl_factor, consistency in settings:
                reset_status = reset_config(repl_factor, consistency)
                tqdm.write(f"\nSetting Replication Factor={repl_factor}, Consistency={consistency}: {reset_status}")

                success, elapsed_time, network_config, key_counter = process_insert_directory(insert_directory)
                if not success:
                    print(f"Failed to fill node with data")
                    os._exit(1)

                success, elapsed_time, network_config, key_counter = process_query_directory(queries_directory)

                if success:
                    results.append([repl_factor, consistency, key_counter, f"{elapsed_time:.2f} sec"])
                else:
                    results.append([repl_factor, consistency, key_counter, "Failed"])

                pbar.update(1)
                tqdm.write("Let the Conchord rest for 1 second.")
                time.sleep(1)

        df = pd.DataFrame(results, columns=["Replication Factor", "Consistency", "Keys Queried", "Time Taken (s)"])
        df["Time Taken (s)"] = df["Time Taken (s)"].str.replace(" sec", "", regex=False).astype(float)
        df["Read Throughput (Queries/sec)"] = df["Keys Queried"] / df["Time Taken (s)"]
        df = df.drop(columns=["Keys Queried"])
        print("\nQuery Experiment Results:")
        print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
        save_results_to_csv(df, "read_throughput_experiment.csv")
        input("\nPress Enter to return to the menu...")
    except Exception as e:
        print(f"Error on batch query: {e}")
        os._exit(1)

def print_menu():
    os.system('clear' if os.name == 'posix' else 'cls')
    print("\n" + "=" * 30)
    print("          CLI MENU         ")
    print("=" * 30)
    print("[1] ➤ Run 1st Experiment (Write Throughput)")
    print("[2] ➤ Run 2nd Experiment (Read Throughput)")
    print("[3] ➤ Run 3nd Experiment (Freshness)")
    print("[4] ➤ Exit")
    print("=" * 30)
    choice = input("Enter choice [1/2/3/4]: ")
    return choice


if __name__ == "__main__":
    while True:
        choice = print_menu()

        if choice == "1":
            insert_directory = input("Enter directory path for batch insert during experiment: ") or "insert"
            print("Will run write throughput experiment for yall!")
            run_insert_experiment(insert_directory)

        elif choice == "2":
            insert_directory = input("Enter directory path for batch insert during experiment: ") or "insert"
            queries_directory = input("Enter directory path for batch queries during experiment: ") or "queries"
            print("Will run read throughput experiment for yall!")
            run_query_experiment(insert_directory, queries_directory)

        elif choice == "3":
            request_directory = input("Enter request directory path for freshness experiment: ") or "requests"
            print("Will run freshness experiment for yall!")
            run_freshness_experiment(request_directory)

        elif choice == "4" or choice.lower() in ["exit", "quit", "q"]:
            print("Exiting...")
            break
        else:
            print("Invalid choice. Try again.")

