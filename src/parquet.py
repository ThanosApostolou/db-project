from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("parquet").getOrCreate()

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
    tdf = spark.read.schema(tschema).csv("hdfs://master:9000/tripdata-min.csv")
    tdf.write.mode("overwrite").parquet("hdfs://master:9000/yellow_tripdata_1m.parquet")

    vschema = StructType([
        StructField("TripID", StringType(), False),
        StructField("VendorID", StringType(), True)
    ])
    vdf = spark.read.schema(vschema).csv("hdfs://master:9000/yellow_tripvendors_1m.csv")
    vdf.write.mode("overwrite").parquet("hdfs://master:9000/yellow_tripvendors_1m.parquet")

    spark.stop()