from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Q1-SQL").getOrCreate()

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

    trips = trips.selectExpr("TripID",
            "date_format(cast(StartDate as Timestamp),'HH') as HourOfDay",
            "(cast(cast(FinishDate as Timestamp)as long) - cast(cast(StartDate as Timestamp)as long))/60 as Duration")
    trips.createOrReplaceTempView("trips")

    result = spark.sql("SELECT HourOfDay, AVG(Duration) AS AverageTripDuration \
                        FROM trips \
                        GROUP BY HourOfDay \
                        ORDER BY HourOfDay")

    result.write.format("csv").mode("overwrite").options(delimiter='\t').save("hdfs://master:9000/Q1-SQL-out")
    result.show()
    spark.stop()