#!/bin/bash

hdfs dfs -rm -r /Q1-MR-out.txt
time spark-submit Q1-MR.py
hdfs dfs -text /Q1-MR-out.txt/*
