#!/bin/bash

INPUT_PATH=${1:-/input/data}

hdfs dfs -rm -r -f /indexer

mapred streaming \
    -D mapreduce.job.reduces=1 \
    -files mapreduce/mapper1.py,mapreduce/reducer1.py \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input $INPUT_PATH \
    -output /indexer

echo "Index created at /indexer"
hdfs dfs -ls /indexer
