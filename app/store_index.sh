#!/bin/bash

source .venv/bin/activate

echo "Waiting for Cassandra..."
until python3 -c "
from cassandra.cluster import Cluster
c = Cluster(['cassandra-server'])
s = c.connect()
c.shutdown()
print('ready')
" 2>/dev/null; do
    sleep 5
done

echo "Loading index into Cassandra..."
hdfs dfs -cat /indexer/part-* | python3 store_index_helper.py
