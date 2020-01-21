from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
import datetime
import math

def map_rides (line) :
    line = line.split(",")
    ride_id = line[0]
    hourofday = line[1].split(" ")[1].split(":")[0]
    finish_datetime = datetime.datetime.strptime(line[2], '%Y-%m-%d %H:%M:%S')
    start_datetime = datetime.datetime.strptime(line[1], '%Y-%m-%d %H:%M:%S')
    start_date = start_datetime.date()

    duration=(finish_datetime - start_datetime).total_seconds() / 60

    l_start = float(line[3])
    f_start = float(line[4])
    l_finish = float(line[5])
    f_finish = float(line[6])
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
    return Row(Ride=ride_id, StartDay=start_date, Speed=speed)

def map_vendors (line) :
    line = line.split(",")
    ride_id = line[0]
    vendor = line[1]
    return Row(Ride=ride_id, Vendor=vendor)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Q2-SQL-PARQUET").getOrCreate()
    sc = spark.sparkContext

    rides = sc.textFile("hdfs://master:9000/yellow_tripdata_1m.csv")
    rides = rides.map(map_rides)
    rides = spark.createDataFrame(rides)
    rides.createOrReplaceTempView("rides")

    fastest_rides = spark.sql("SELECT Ride, Speed \
                               FROM rides \
                               WHERE StartDay > '2015-03-10' \
                               ORDER BY Speed DESC \
                               LIMIT 5")
    fastest_rides.createOrReplaceTempView("fastest_rides")

    vendors = sc.textFile("hdfs://master:9000/yellow_tripvendors_1m.csv")
    vendors = vendors.map(map_vendors)
    vendors = spark.createDataFrame(vendors)
    vendors.createOrReplaceTempView("vendors")

    result = spark.sql("SELECT fastest_rides.Ride, Speed, Vendor \
                        FROM fastest_rides \
                        LEFT JOIN vendors \
                        ON fastest_rides.Ride = vendors.Ride \
                        ORDER BY Speed DESC")

    result.write.format("csv").mode("overwrite").options(delimiter='\t').save("hdfs://master:9000/Q2-SQL-PARQUET-out")
    result.show()
    spark.stop()