#!/bin/bash

# Define the IPs for each VM
declare -a IPS=("10.0.9.91" "10.0.9.86" "10.0.9.176" "10.0.9.31" "10.0.9.160")

# Kill all node.py processes on each VM
for i in {0..4}; do
    VM="team_2-vm$((i+1))"
    IP="${IPS[$i]}"  # Assign the corresponding IP

    echo "Stopping nodes on $VM ($IP)..."
    ssh $VM "pkill -f node.py" &
done

wait  # Ensures all SSH sessions finish
echo "All nodes stopped."
