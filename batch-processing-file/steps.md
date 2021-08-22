## Steps for Batch Processing
#### 1. Create Azure Synpase workspace 
       Also create dedicated sql pool named 'edw'
#### 2. Upload the data using MS Storage explorer
#### 3. Create a blob storage and container in it
#### 4. Create azure key vault workspace 
     - Give Access to Databricks & Azure Synapse analytics workspace    
     - Add Secrets 
                 a. azure-datalake-access
                 b. username and password of dedicated sql pool  (azure-sql-pwd & azure-sql-username)
                 c. databricks-synapse-access
                 d. azure-blob-access
     
 #### 5. Create databricks workspace - e.g. datatechdb
 #### 6. create databricks cluster 
       - Add vault info to databricks  #secrets/createScope 
            Name - datatech
            Copy the following info from Key-vault properties 
                    Vault URI
                    Resource ID
        - create a token to access synpase (store in Azure key vault)  name it databricks-access-synapse  
        - Import & execute the dataprocessing script 
 #### 7. Create customer & watermark tables in edw
 #### 8. Create link services blob , azure keyvault and databricks
 #### 9. Create First Pipeline to copy data from ADLS to datawarehouse      
           - create two integration datasets
              a. datalake storage with parameters ( Container, Dir_name, File_name)
              b. customer table from Azure synapse dedciated SQL pool
           - Step 1 Lookup with Query
           - Foreach with variables and output from lookup value (@activity('Get_date_list').output.value)
           - Set pipeline variables ( 
                                      DATA_DIR - data/customer/2021/01
                                      FILE_NAME - customers_2021-01-01
                                     )
           - 3 activities in Foreach ( Two Set variables and copydata )
             - Set_DIR_NAME 
                - Set varaibles DATA_DIR - @item().DATA_DIR
             - Set_FILE_NAME
                - Set varaibles FILE_NAME - @item().FILE_NAME
             - Copy activity
                - source datalake integrration dataset
                 - please set 
                     directory - @variables('DATA_DIR') (dynamic)
                     file_name - @variables('FILE_NAME') (dynamic)
                     preview the data 
 #### 10. Create Second Pipeline to run databricks notebook
