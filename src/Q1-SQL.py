from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
import datetime

def map_rides (line) :
    line = line.split(",")
    ride_id = line[0]
    hourofday = line[1].split(" ")[1].split(":")[0]
    finish_datetime = datetime.datetime.strptime(line[2], '%Y-%m-%d %H:%M:%S')
    start_datetime = datetime.datetime.strptime(line[1], '%Y-%m-%d %H:%M:%S')
    ride_duration=(finish_datetime - start_datetime).total_seconds() / 60
    myrow = Row(id=ride_id, HourOfDay=hourofday, duration=ride_duration)
    return myrow

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Q1-SQL").getOrCreate()
    sc = spark.sparkContext

    rides = sc.textFile("hdfs://master:9000/yellow_tripdata_1m.csv")
    rides = rides.map(map_rides)

    schemaRides = spark.createDataFrame(rides)
    schemaRides.createOrReplaceTempView("rides")

    result = spark.sql("SELECT HourOfDay, AVG(duration) AS AverageTripDuration \
                        FROM rides \
                        GROUP BY HourOfDay \
                        ORDER BY HourOfDay")

    result.write.format("csv").mode("overwrite").options(delimiter='\t').save("hdfs://master:9000/Q1-SQL-out")
    result.show()
    spark.stop()