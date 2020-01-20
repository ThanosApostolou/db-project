from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
#from pyspark.sql.types import *
#from pyspark.sql.functions import udf
import sys
import datetime

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Q1-SQL").getOrCreate()
    sc = spark.sparkContext

    rides = sc.textFile("hdfs://master:9000/yellow_tripdata_1m.csv")
    rides = rides.map(lambda l: l.split(","))
    rides = rides.map(lambda l: Row(id=l[0], \
                                HourOfDay = l[1].split(" ")[1].split(":")[0], \
                                duration=(datetime.datetime.strptime(l[2], '%Y-%m-%d %H:%M:%S') - datetime.datetime.strptime(l[1], '%Y-%m-%d %H:%M:%S')).total_seconds() / 60))

    schemaRides = spark.createDataFrame(rides)
    schemaRides.createOrReplaceTempView("rides")

    result = spark.sql("SELECT HourOfDay, AVG(duration) AS AverageTripDuration \
                        FROM rides \
                        GROUP BY HourOfDay \
                        ORDER BY HourOfDay")

    result.write.format("csv").mode("overwrite").options(delimiter='\t').save("hdfs://master:9000/Q1-SQL-out")
    result.show()
    spark.stop()