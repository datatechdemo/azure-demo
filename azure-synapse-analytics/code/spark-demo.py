## Spark pool

%%pyspark
df = spark.read.load('abfss://data@datatechstorage.dfs.core.windows.net/NYCTripSmall.parquet', format='parquet')
df.printSchema()
display(df.limit(10))



spark.sql("CREATE DATABASE IF NOT EXISTS nyctaxi")
df.repartition(1)
df.write.mode("overwrite").saveAsTable("nyctaxi.trip")
spark.sql("Select count(*) from nyctaxi.trip")



df = spark.sql("""
   SELECT PassengerCount,
       SUM(TripDistanceMiles) as SumTripDistance,
       AVG(TripDistanceMiles) as AvgTripDistance
   FROM nyctaxi.trip
   WHERE TripDistanceMiles > 0 AND PassengerCount > 0
   GROUP BY PassengerCount
   ORDER BY PassengerCount
""") 
display(df)
df.write.saveAsTable("nyctaxi.passengercountstats")

