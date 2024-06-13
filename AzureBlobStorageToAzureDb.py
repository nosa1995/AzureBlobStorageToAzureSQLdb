#!/usr/bin/env python
# coding: utf-8

# ## AzureBlobStorageToAzureDb
# 
# New notebook

# In[11]:


# Import necessary libraries
from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("AzureBlobAccess") \
    .getOrCreate()

# Set up the necessary configuration for accessing Azure Blob Storage using SAS token
spark.conf.set("fs.azure.account.auth.type.nosa-datalake.blob.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.nosa-datalake.blob.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.nosa-datalake.blob.core.windows.net", "sp=r&st=2024-06-12T004KjEt4SeSK17QeMmBGi7rTKsaHZtoeUBXDg%3D")

# Define the path for the Parquet file
container_name = 'nosa-container'
account_name = 'nosa-datalake'
file_path = 'customerdata.snappy.parquet'
sas_token = 'sp=r&st=2024-06-12T004KjEt4SeSK17QeMmBGi7rTKsaHZtoeUBXDg%3D'
wasbs_path = f'wasbs://{container_name}@{account_name}.blob.core.windows.net/{file_path}'

# Read the Parquet file from Azure Blob Storage
df = spark.read.parquet(wasbs_path)

# Show the data (or perform any DataFrame operations)
df.show(2)


# In[6]:


spark.conf.set("sprk.sql.parquet.vorder.enabled", "true") # Enable Verti-Parquet write
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true") # Enable automatic delta optimized write


# In[13]:


from pyspark.sql import SparkSession
# Initialize Spark session
spark = SparkSession.builder \
    .appName("AzureSQLNewTableWrite") \
    .getOrCreate()

# Prepare the JDBC URL with credentials to DB
jdbc_url = (
    "jdbc:sqlserver://nosa-practice.database.windows.net;"  # server name
    "databaseName=Nosadb;"
    "user=nosa;"
    "password=123;"
)
from pyspark.sql.functions import col, to_timestamp, current_timestamp, year, month
 # Add dataload_datetime column with current timestamp
df = df.withColumn("ExtractedDateTime", current_timestamp())
# Convert pandas DataFrame to Spark DataFrame


# Define the new table name to write to
new_table_name = "dbo.tbl_customer"  # Adjust schema and table name as necessary

# Write the Spark DataFrame to the new Azure SQL Database table
df.write \
    .format("jdbc") \
    .mode("append") \
    .option("url", jdbc_url) \
    .option("dbtable", new_table_name) \
    .option("user", "noza") \
    .option("password", "6007521sH!") \
    .save()

print(f"DataFrame written to new Azure SQL Database table: {new_table_name}")


