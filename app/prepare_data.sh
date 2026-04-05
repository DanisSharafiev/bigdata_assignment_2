#!/bin/bash

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

hdfs dfs -mkdir -p /data
hdfs dfs -put -f data/* /data/
hdfs dfs -rm -r -f /input/data

spark-submit transform_data.py

hdfs dfs -ls /input/data
echo "done data preparation!"
