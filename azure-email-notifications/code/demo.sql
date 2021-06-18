#### Spark Pool Code ####

## Change your linked service name

df = spark.read\
    .format("cosmos.olap")\
    .option("spark.synapse.linkedService", "CosmosWebDataOLAP")\
    .option("spark.cosmos.container", "WebsiteData")\
    .load()

df.printSchema()

df.show(3000, False)

from pyspark.sql.functions import *
aggdf = df.groupBy(col('Item')).agg(sum(col('Price')).alias('sum_price'))

aggdf.write.mode("overwrite").saveAsTable("default.spark2")




#### Azure Cosmos DB


database - RetailDemo
container - WebsiteData
partitionby - /CartID

import azure.cosmos
from azure.cosmos.partition_key import PartitionKey
 
database = cosmos_client.get_database_client('RetailDemo')
print('Database RetailDemo Read')
 
container = database.get_container_client('WebsiteData')
print('Container WebsiteData Read')

%%upload --databaseName RetailDemo --containerName WebsiteData --url https://cosmosnotebooksdata.blob.core.windows.net/notebookdata/websiteData.json
