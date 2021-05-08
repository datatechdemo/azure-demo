from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, year, month, dayofmonth, unix_timestamp, round, when
from datetime import datetime

# Replace your storage location
greentaxidf = spark.read.load('abfss://users@datatechdemostorage.dfs.core.windows.net/taxidata/GreenTaxiTripData_201812.csv', format='csv', header=True)
display(greentaxidf.limit(10))

greentaxidf = greentaxidf.where(col('passenger_count') > 0) \
                      .filter(col('trip_distance') > 0.0) \
                      .dropDuplicates() 


print("Extracted & cleaned Green Taxi data")
greentaxidf.printSchema()

greentaxidf = greentaxidf.select( col("VendorID"),
                                  col("passenger_count").alias("PassengerCount"),
                                  col("trip_distance").alias("TripDistance"),
                                  col("lpep_pickup_datetime").alias("PickupTime"),                          
                                  col("lpep_dropoff_datetime").alias("DropTime"), 
                                  col("PUlocationID").alias("PickupLocationId"), 
                                  col("DOlocationID").alias("DropLocationId"), 
                                  col("RatecodeID"), 
                                  col("total_amount").alias("TotalAmount"),
                                  col("payment_type").alias("PaymentType")
                                  ) \
                           .withColumn("TripYear", year("PickupTime")) \
                           .withColumn("TripMonth", month("PickupTime")) \
                           .withColumn("TripDay", dayofmonth("PickupTime")) \
                           .withColumn("TripTimeInMinutes", round((unix_timestamp(col("DropTime")) - unix_timestamp(col("PickupTime")) / 60))) \
                           .withColumn("TripType", when(col("RatecodeID") == 6,"SharedTrip").otherwise("SoloTrip")) \
                           .drop("RatecodeID")

greentaxidf.printSchema()                

## Store the DataFrame as an managed spark Table
spark.sql("CREATE DATABASE IF NOT EXISTS nyctaxi")
greentaxidf.repartition(4).write.mode("overwrite").saveAsTable("nyctaxi.greentrip")
