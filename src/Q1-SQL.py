from __future__ import print_function
from pyspark import SparkContext
import sys
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

def mapper2 (line) :
    return ("" + line[0] + "\t\t" + str(line[1][0]/line[1][1]))

def myprint (line) :
    print (line)

if __name__ == "__main__":
    sc = SparkContext(appName="Q1-MR")

    final_rdd = sc.parallelize(["HourOfDay\tAverageTripDuration"])
    text_file = sc.textFile("hdfs://master:9000/yellow_tripdata_1m.csv")
    mean_times = text_file.map(mapper).reduceByKey(reducer).sortByKey().map(mapper2)
    final_rdd = final_rdd.union(mean_times)

    final_rdd.saveAsTextFile("hdfs://master:9000/Q1-MR-out")
    for x in final_rdd.collect() :
        print (x)