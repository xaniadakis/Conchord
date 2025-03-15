#!/bin/bash

# Set the number of nodes per VM (default is 1 if not provided)
NODES_PER_VM=${1:-2}
REPLICATION_FACTOR=${2:-3}  # Default replication factor is 3
CONSISTENCY_TYPE=${3:-chain}  # Default consistency type is 'chain'

# Define the IPs for each VM
declare -a IPS=("10.0.9.91" "10.0.9.86" "10.0.9.176" "10.0.9.31" "10.0.9.160")

for i in {0..4}; do
    VM="team_2-vm$((i+1))"
    IP="${IPS[$i]}"

    for j in $(seq 0 $((NODES_PER_VM - 1))); do
        PORT=$((5000 + i * NODES_PER_VM + j))

        if [[ $i -eq 0 && $j -eq 0 ]]; then
            # Start the bootstrap node with user-defined replication factor and consistency type
            ssh $VM "python3 ~/conchord/node.py --ip $IP --port $PORT --bootstrap --replication_factor $REPLICATION_FACTOR --consistency $CONSISTENCY_TYPE" &
            sleep 2  # Ensure bootstrap starts before other nodes
        else
            # Start other nodes and connect to the bootstrap node
            ssh $VM "python3 ~/conchord/node.py --ip $IP --port $PORT --bootstrap_ip 10.0.9.91 --bootstrap_port 5000" &
        fi
        sleep 0.5  # 💤 Prevent too many SSH requests at once
    done
done

wait  # Ensures all SSH sessions finish
