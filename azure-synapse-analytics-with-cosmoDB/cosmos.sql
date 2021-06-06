SELECT TOP 10 *
FROM OPENROWSET( 
       'CosmosDB',
       'Account=datatechcosmo;Database=OLAP;Key=LCbCTJntgDwfTfSiAt6sLvTOV5RjgA3OXraLDJ3ApDn4TRxrH4atkqDarciec4S8YAZFNIrNySGFPGvYTd6IXQ==',
       families) as documents