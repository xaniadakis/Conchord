if ! command -v parallel &> /dev/null; then
    echo "GNU Parallel is not installed. Installing now..."
    sudo apt update && sudo apt install -y parallel
else
    echo "GNU Parallel is already installed."
fi
parallel -v scp -r ./node.py team_2-vm{}:~/conchord ::: {1..5}
