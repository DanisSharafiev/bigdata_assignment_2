#!/bin/bash

echo "=== Creating index ==="
bash create_index.sh "$@" || exit 1

echo "=== Storing index in Cassandra ==="
bash store_index.sh || exit 1

echo "=== Indexing complete ==="
