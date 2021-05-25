from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName('File sink') \
                               .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    schema = StructType([StructField('car', StringType(), True),\
                         StructField('price', DoubleType(), True),\
                         StructField('body', StringType(), True),\
                         StructField('mileage', DoubleType(), True),\
                         StructField('engV', StringType(), True),\
                         StructField('engType', StringType(), True),\
                         StructField('registration', StringType(), True),\
                         StructField('year', IntegerType(), True),\
                         StructField('model', StringType(), True),\
                         StructField('drive', StringType(), True)])
    # Replace the path with your location of ADLS2 in Azure synpase analytics  
    fileStreamDf = spark.readStream \
			 .option('header', 'true')\
			 .schema(schema)\
			 .csv("path")

    selectedDf = fileStreamDf.select('*')

    cleanDf = selectedDf.select('car','price','body','mileage','engType','year','model','drive') \
                         .withColumnRenamed('car','make') \
                         .withColumnRenamed('mileage', 'distance')
    
    # Replace the path with your location of ADLS2 in Azure synpase analytics  
    query = cleanDf.writeStream.outputMode('append') \
                .format('json') \
                .option('path','abfss://users@datatechstorage.dfs.core.windows.net/output_data/') \
                .option('checkpointLocation','path') \
                .start()
   
    query.awaitTermination()
