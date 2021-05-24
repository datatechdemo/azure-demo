from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.master("local") \
                               .appName("Console sink") \
                               .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    schema = StructType([StructField("Date", StringType(), False), \
    					 StructField("Article_ID", StringType(), False), \
    					 StructField("Country_Code", StringType(), False), \
    					 StructField("Sold_Units", IntegerType(), False)])
    
    fileStreamDf = spark.readStream \
					 		   .option("header", "true")\
					 		   .schema(schema)\
					 		   .csv("abfss://users@datatechstorage.dfs.core.windows.net/streaming_data")

    aggDf = fileStreamDf.select('Article_ID','Country_Code','Sold_Units') 

    query = aggDf.writeStream.outputMode('append') \
                .format('csv') \
                .option('path','abfss://users@datatechstorage.dfs.core.windows.net/output_data') \
                .option('checkpointLocation','abfss://users@datatechstorage.dfs.core.windows.net/checkpoint') \
                .start()
    query.awaitTermination()
