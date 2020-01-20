from __future__ import print_function
from pyspark import SparkContext
import sys
import datetime

def filter_date (line) :
    line_list = line.split(",")
    start_datetime = line_list[1]
    sd = datetime.datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S')
    finish_datetime = line_list[2]
    fd = datetime.datetime.strptime(finish_datetime, '%Y-%m-%d %H:%M:%S')
    start_date = sd.date()
    the_date = datetime.datetime.strptime("2015-03-10", '%Y-%m-%d')
    the_date = the_date.date()
    return (start_date > the_date)

def mapper (line) :
    line_list = line.split(",")
    ride_id = line_list[0]
    start_datetime = line_list[1]
    sd = datetime.datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S')
    finish_datetime = line_list[2]
    fd = datetime.datetime.strptime(finish_datetime, '%Y-%m-%d %H:%M:%S')
    duration = (fd - sd).total_seconds() / 60
    return (ride_id, duration)

def vendor_data (line) :
    line_list = line.split(",")
    ride_id = line_list[0]
    vendor = line_list[1]
    return (ride_id, vendor)

if __name__ == "__main__":
    sc = SparkContext(appName="Q2-MR")

    final_rdd = sc.parallelize(["Ride\t\tDuration\tVendor"])
    faster_rides = sc.textFile("hdfs://master:9000/tripdata-min.csv")
    faster_rides = faster_rides.filter(filter_date).map(mapper)
    faster_rides = faster_rides.sortBy(lambda line : line[1]).take(5)
    rides_list = [ x[0] for x in faster_rides ]
    faster_rides = sc.parallelize(faster_rides)

    vendors = sc.textFile("hdfs://master:9000/yellow_tripvendors_1m.csv")
    vendors = vendors.map(vendor_data)
    vendors = vendors.filter(lambda line : line[0] in rides_list )

    faster_rides = faster_rides.leftOuterJoin(vendors).sortBy(lambda line : line[1][0])
    faster_rides = faster_rides.map(lambda line : line[0]+'\t'+ '%.6f' % line[1][0]+'\t'+line[1][1])

    final_rdd = final_rdd.union(faster_rides)

    final_rdd.saveAsTextFile("hdfs://master:9000/Q2-MR-out")
    for line in final_rdd.collect() :
        print (line)