# !/usr/bin/python
# coding=utf-8
# -*- coding: utf-8 -*-

from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import datetime

def mapper (line) :
    line_list = line.split(",")

    start_date = line_list[1]
    sd = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    finish_date = line_list[2]
    fd = datetime.datetime.strptime(finish_date, '%Y-%m-%d %H:%M:%S')

    duration = (fd - sd).total_seconds() / 60
    start_hour = start_date.split(" ")[1].split(":")[0]

    return (start_hour, (duration, 1))

def reducer (a, b) :
    return (a[0]+b[0],a[1]+b[1])

if __name__ == "__main__":
    sc = SparkContext(appName="Q1-MR")

    text_file = sc.textFile("hdfs://master:9000/tripdata-min.csv")

    counts = text_file.map(mapper).reduceByKey(reducer).sortByKey()
    print("Saving to file Q1-MR-out.txt\n")
    counts.saveAsTextFile("hdfs://master:9000/Q1-MR-out.txt")
