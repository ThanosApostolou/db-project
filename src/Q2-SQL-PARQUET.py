from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import sys
import datetime
import math

def calculate_duration (sd, fd) :
    finish_datetime = datetime.datetime.strptime(fd, '%Y-%m-%d %H:%M:%S')
    start_datetime = datetime.datetime.strptime(sd, '%Y-%m-%d %H:%M:%S')
    duration=(finish_datetime - start_datetime).total_seconds() / 60
    return duration

def calculate_distance (ls, fs, lf, ff) :
    l_start = float(ls)
    f_start = float(fs)
    l_finish = float(lf)
    f_finish = float(ff)
    Df = f_finish - f_start
    Dl = l_finish - l_start
    a = math.pow(math.sin(Df/2),2) + math.cos(f_start) * math.cos(f_finish) * math.pow(math.sin(Dl/2),2)
    c = math.atan2(math.sqrt(a), math.sqrt(1-a))
    R = 6371
    distance = R * c
    return distance

def calculate_speed (sd, fd, ls, fs, lf, ff) :
    duration = calculate_duration(sd, fd)
    distance = calculate_distance(ls, fs, lf, ff)
    if (duration == 0) :
        speed = 0
    else :
        speed = distance / duration
    return speed

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Q2-SQL-PARQUET").getOrCreate()

    trips = spark.read.parquet("hdfs://master:9000/yellow_tripdata_1m.parquet")

    speedUdf = udf(calculate_speed, DoubleType())
    trips = trips.filter("cast(StartDate as DATE) > cast('2015-03-10' as DATE)"). \
            withColumn("Speed", speedUdf("StartDate", "FinishDate", "StartLongitude",
                             "StartLatitude", "FinishLongitude", "FinishLatitude")). \
            select("TripID", "Speed").orderBy("Speed", ascending=False).limit(5)
    trips.createOrReplaceTempView("trips")

    vendors = spark.read.parquet("hdfs://master:9000/yellow_tripvendors_1m.parquet")
    vendors.createOrReplaceTempView("vendors")

    result = spark.sql("SELECT trips.TripID, Speed, vendors.VendorID \
                        FROM trips \
                        LEFT JOIN vendors \
                        ON trips.TripID = vendors.TripID \
                        ORDER BY Speed DESC")

    result.write.format("csv").mode("overwrite").options(delimiter='\t').save("hdfs://master:9000/Q2-SQL-PARQUET-out")
    result.show()
    spark.stop()