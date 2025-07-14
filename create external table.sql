
-- create master key --
CREATE MASTER KEY ENCRYPTION BY PASSWORD ='12345678aA' ;

-- Create external credential--
CREATE DATABASE SCOPED CREDENTIAL cred_aarti2
WITH 
IDENTITY = 'Managed Identity'

--Create External Data Source--
CREATE EXTERNAL DATA SOURCE source_silver3
WITH ( 
    LOCATION = 'https://asdatalake1.blob.core.windows.net/silver/AdventureWorks_Sales',
    CREDENTIAL = cred_aarti2
    )

--Create External Data Gold--2
CREATE EXTERNAL DATA SOURCE source_gold3
WITH ( 
    LOCATION = 'https://asdatalake1.blob.core.windows.net/gold',
    CREDENTIAL = cred_aarti2
    )

CREATE EXTERNAL FILE FORMAT format_parquet
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

CREATE EXTERNAL TABLE gold.extsales3
WITH(
    LOCATION = 'extsales3',
    DATA_SOURCE = source_gold3,
    FILE_FORMAT = format_parquet
)as SELECT * FROM gold.sales

SELECT * from gold.extsales3