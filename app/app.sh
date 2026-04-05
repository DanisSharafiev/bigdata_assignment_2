#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Upgrade pip and install wheel first
pip install --upgrade pip wheel

# Install any packages
pip install -r requirements.txt

# Package the virtual env.
venv-pack -o .venv.tar.gz

# Collect data
bash prepare_data.sh


# Run the indexer
bash index.sh

# Run the ranker
echo "=== Query 1: history war ==="
bash search.sh "history war"

echo "=== Query 2: music album ==="
bash search.sh "music album"

echo "=== Query 3: film director ==="
bash search.sh "film director"
