from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import sys
import datetime
import math

def calculate_speed (start_datetime, finish_datetime, l_start, f_start, l_finish, f_finish) :
    finish_datetime = datetime.datetime.strptime(finish_datetime, '%Y-%m-%d %H:%M:%S')
    start_datetime = datetime.datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S')
    duration=(finish_datetime - start_datetime).total_seconds() / 60

    l_start = float(l_start)
    f_start = float(f_start)
    l_finish = float(l_finish)
    f_finish = float(f_finish)
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
    return speed

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Q2-SQL").getOrCreate()

    tschema = StructType([
        StructField("TripID", StringType(), False),
        StructField("StartDate", StringType(), True),
        StructField("FinishDate", StringType(), True),
        StructField("StartLongitude", StringType(), True),
        StructField("StartLatitude", StringType(), True),
        StructField("FinishLongitude", StringType(), True),
        StructField("FinishLatitude", StringType(), True),
        StructField("Cost", StringType(), True)
    ])
    trips = spark.read.schema(tschema).csv("hdfs://master:9000/yellow_tripdata_1m.csv")

    speedUdf = udf(calculate_speed)
    trips = trips.withColumn("Speed", speedUdf("StartDate", "FinishDate", "StartLongitude",
                             "StartLatitude", "FinishLongitude", "FinishLatitude"))
    trips = trips.selectExpr("TripID", "Speed", "cast(StartDate as DATE)")
    trips.createOrReplaceTempView("trips")

    trips = spark.sql("SELECT TripID, Speed \
                      FROM trips \
                      WHERE StartDate > '2015-03-10' \
                      ORDER BY Speed DESC \
                      LIMIT 5")
    trips.createOrReplaceTempView("trips")

    vschema = StructType([
        StructField("TripID", StringType(), False),
        StructField("VendorID", StringType(), True)
    ])
    vendors = spark.read.schema(vschema).csv("hdfs://master:9000/yellow_tripvendors_1m.csv")
    vendors.createOrReplaceTempView("vendors")

    result = spark.sql("SELECT trips.TripID, Speed, vendors.VendorID \
                        FROM trips \
                        LEFT JOIN vendors \
                        ON trips.TripID = vendors.TripID \
                        ORDER BY Speed DESC")

    result.write.format("csv").mode("overwrite").options(delimiter='\t').save("hdfs://master:9000/Q2-SQL-out")
    result.show()
    spark.stop()