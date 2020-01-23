#!/bin/bash
#hdfs dfs -get /Q1-MR-out ./output
#hdfs dfs -text /Q1-MR-out/* > ./output/Q1-MR-out.txt

if [ "$1" = "1a" ]; then
    hdfs dfs -rm -r /Q1-MR-out
    time spark-submit ./src/Q1-MR.py 2>&1 | tee ./logs/Q1-MR/master.log
elif [ "$1" = "1b" ]; then
    time spark-submit ./src/Q1-SQL.py 2>&1 | tee ./logs/Q1-SQL/master.log
elif [ "$1" = "1c" ]; then
    time spark-submit ./src/Q1-SQL-PARQUET.py 2>&1 | tee ./logs/Q1-PARQUET-SQL/master.log
elif [ "$1" = "2a" ]; then
    hdfs dfs -rm -r /Q2-MR-out
    time spark-submit ./src/Q2-MR.py 2>&1 | tee ./logs/Q2-MR/master.log
elif [ "$1" = "2b" ]; then
    time spark-submit ./src/Q2-SQL.py 2>&1 | tee ./logs/Q2-SQL/master.log
elif [ "$1" = "2c" ]; then
    time spark-submit ./src/Q2-SQL-PARQUET.py 2>&1 | tee ./logs/Q2-SQL-PARQUET/master.log
elif [ "$1" = "parquet" ]; then
    hdfs dfs -rm -r /yellow_tripdata_1m.parquet
    hdfs dfs -rm -r /yellow_tripvendors_1m.parquet
    time spark-submit ./src/parquet.py 2>&1 | tee ./logs/parquet/master.log
else
    echo "Usage: run.sh {1a,1b,1c,2a,2b,2c,parquet}"
fi
