#!/bin/bash

export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

PARQUET_PATH=${1:-/y.parquet}

if [ ! -f "$PARQUET_PATH" ]; then
    echo "Parquet file not found at $PARQUET_PATH, using existing files in data/"
else
    spark-submit prepare_data.py "$PARQUET_PATH"
fi

hdfs dfs -mkdir -p /data
hdfs dfs -put -f data/* /data/
hdfs dfs -rm -r -f /input/data

spark-submit transform_data.py

hdfs dfs -ls /input/data
echo "done data preparation!"
