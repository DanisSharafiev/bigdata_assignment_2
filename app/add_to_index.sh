#!/bin/bash

source .venv/bin/activate

FILE=$1
if [ -z "$FILE" ]; then
    echo "Usage: bash add_to_index.sh <path_to_file>"
    exit 1
fi

FNAME=$(basename "$FILE" .txt)

hdfs dfs -put -f "$FILE" /data/

python3 -c "
import re, sys
from cassandra.cluster import Cluster

fname = '$FNAME'
doc_id = int(fname.split('_', 1)[0])
title = fname.split('_', 1)[1].replace('_', ' ') if '_' in fname else ''

with open('$FILE') as f:
    text = f.read()

tokens = re.findall(r'[a-z0-9]+', text.lower())
dl = len(tokens)

tf = {}
for t in tokens:
    tf[t] = tf.get(t, 0) + 1

cluster = Cluster(['cassandra-server'])
s = cluster.connect('search_engine')

s.execute('INSERT INTO documents (doc_id, title, doc_length) VALUES (%s, %s, %s)', (doc_id, title, dl))

for term, count in tf.items():
    s.execute('INSERT INTO inverted_index (term, doc_id, tf) VALUES (%s, %s, %s)', (term, doc_id, count))
    row = s.execute('SELECT df FROM vocabulary WHERE term = %s', (term,)).one()
    new_df = (row.df + 1) if row else 1
    s.execute('INSERT INTO vocabulary (term, df) VALUES (%s, %s)', (term, new_df))

row = s.execute('SELECT num_docs, avg_dl FROM collection_stats WHERE id = 1').one()
new_n = row.num_docs + 1
new_avg = (row.avg_dl * row.num_docs + dl) / new_n
s.execute('INSERT INTO collection_stats (id, num_docs, avg_dl) VALUES (%s, %s, %s)', (1, new_n, new_avg))

cluster.shutdown()
print(f'Added doc {doc_id}: {title}')
"
