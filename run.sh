#!/bin/bash
#hdfs dfs -get /Q1-MR-out ./output
#hdfs dfs -text /Q1-MR-out/* > ./output/Q1-MR-out.txt

if [ "$1" = "1a" ]; then
    hdfs dfs -rm -r /Q1-MR-out
    time spark-submit ./src/Q1-MR.py
elif [ "$1" = "1b" ]; then
    #hdfs dfs -rm -r /Q1-SQL-out
    time spark-submit ./src/Q1-SQL.py
elif [ "$1" = "2a" ]; then
    hdfs dfs -rm -r /Q2-MR-out
    time spark-submit ./src/Q2-MR.py
elif [ "$1" = "2b" ]; then
    time spark-submit ./src/Q2-SQL.py
else
    echo "Usage: run.sh {1a}"
fi
