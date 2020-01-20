from __future__ import print_function
from pyspark import SparkContext
import sys
import datetime
import math

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

def map_ride_speed (line) :
    line_list = line.split(",")
    ride_id = line_list[0]
    start_datetime = line_list[1]
    sd = datetime.datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S')
    finish_datetime = line_list[2]
    fd = datetime.datetime.strptime(finish_datetime, '%Y-%m-%d %H:%M:%S')
    duration = (fd - sd).total_seconds() / 60

    l_start = float(line_list[3])
    f_start = float(line_list[4])
    l_finish = float(line_list[5])
    f_finish = float(line_list[6])
    Df = f_finish - f_start
    Dl = l_finish - l_start
    a = math.sin(Df/2)**2 + math.cos(f_start) * math.cos(f_finish) * (math.sin(Dl/2)**2)
    c = math.atan2(math.sqrt(a), math.sqrt(1-a))
    R = 6371
    distance = R * c
    if (duration == 0) :
        speed = 0
    else :
        speed = distance / duration
    return (ride_id, speed)

def map_vendor_data (line) :
    line_list = line.split(",")
    ride_id = line_list[0]
    vendor = line_list[1]
    return (ride_id, vendor)

if __name__ == "__main__":
    sc = SparkContext(appName="Q2-MR")

    final_rdd = sc.parallelize(["Ride\t\tSpeed\t\tVendor"])
    faster_rides = sc.textFile("hdfs://master:9000/yellow_tripdata_1m.csv")
    faster_rides = faster_rides.filter(filter_date).map(map_ride_speed)
    faster_rides = faster_rides.sortBy(lambda line : line[1], False).take(5)
    rides_list = [ x[0] for x in faster_rides ]
    faster_rides = sc.parallelize(faster_rides)

    vendors = sc.textFile("hdfs://master:9000/yellow_tripvendors_1m.csv")
    vendors = vendors.map(map_vendor_data)
    vendors = vendors.filter(lambda line : line[0] in rides_list )

    faster_rides = faster_rides.leftOuterJoin(vendors).sortBy(lambda line : line[1][0], False)
    faster_rides = faster_rides.map(lambda line : line[0]+'\t'+ str(line[1][0])+'\t'+line[1][1])

    final_rdd = final_rdd.union(faster_rides)

    final_rdd.saveAsTextFile("hdfs://master:9000/Q2-MR-out")
    for line in final_rdd.collect() :
        print (line)