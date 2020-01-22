#!/bin/bash
#hdfs dfs -get /Q1-MR-out ./output
#hdfs dfs -text /Q1-MR-out/* > ./output/Q1-MR-out.txt

if [ "$1" = "1a" ]; then
    hdfs dfs -rm -r /Q1-MR-out
    time spark-submit ./src/Q1-MR.py
elif [ "$1" = "1b" ]; then
    time spark-submit ./src/Q1-SQL.py
elif [ "$1" = "1c" ]; then
    time spark-submit ./src/Q1-SQL-PARQUET.py
elif [ "$1" = "2a" ]; then
    hdfs dfs -rm -r /Q2-MR-out
    time spark-submit ./src/Q2-MR.py
elif [ "$1" = "2b" ]; then
    time spark-submit ./src/Q2-SQL.py
elif [ "$1" = "2c" ]; then
    time spark-submit ./src/Q2-SQL-PARQUET.py
elif [ "$1" = "parquet" ]; then
    time spark-submit ./src/parquet.py
else
    echo "Usage: run.sh {1a,1b,1c,2a,2b,2c,parquet}"
fi
