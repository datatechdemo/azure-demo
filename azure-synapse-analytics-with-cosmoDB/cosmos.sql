SELECT TOP 10 *
FROM OPENROWSET( 
       'CosmosDB',
       'Account=datatechcosmo;Database=OLAP;Key=<Enter your Key>',
       families) as documents
