#!/bin/bash
#hdfs dfs -get /Q1-MR-out ./out/
#hdfs dfs -text /Q1-MR-out/* > ./out/Q1-MR-out.txt

if [ "$1" = "1a" ]; then
    hdfs dfs -rm -r /Q1-MR-out
    time spark-submit ./src/Q1-MR.py
else
    echo "Usage: run.sh {1a}"
fi
