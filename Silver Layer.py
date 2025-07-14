# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Accessing 

# COMMAND ----------



spark.conf.set("fs.azure.account.auth.type.asdatalake1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.asdatalake1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.asdatalake1.dfs.core.windows.net", "1d236bc0-f85d-466f-b9e4-f2486bc81172")
spark.conf.set("fs.azure.account.oauth2.client.secret.asdatalake1.dfs.core.windows.net", "cdM8Q~ThY4ZPoNWi7gL0Rv46~qHfyIyAacnimcDj")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.asdatalake1.dfs.core.windows.net", "https://login.microsoftonline.com/f88d4cbd-512f-43df-9e3e-77b1efc13a1d/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Loading

# COMMAND ----------

df_cal = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load("abfss://bronze@asdatalake1.dfs.core.windows.net/AdventureWorks_Calendar")
        

# COMMAND ----------

df_cus = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load("abfss://bronze@asdatalake1.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_procat = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load("abfss://bronze@asdatalake1.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

df_pro = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load("abfss://bronze@asdatalake1.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_ret = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load("abfss://bronze@asdatalake1.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

df_sales = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load("abfss://bronze@asdatalake1.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

df_ter = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load("abfss://bronze@asdatalake1.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

df_subcat = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load("abfss://bronze@asdatalake1.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

df_sales = spark.read.format('csv')\
        .option("header", True)\
        .option("inferSchema", True)\
        .load("abfss://bronze@asdatalake1.dfs.core.windows.net/AdventureWorks_Sales_2015")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation

# COMMAND ----------

df_cal.display(
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation for Calendar

# COMMAND ----------

# MAGIC %md
# MAGIC To create month and year column in calendar csv.

# COMMAND ----------

df_cal = df.withColumn('Month',month(col('Date')))\
           .withColumn('Year',year(col('Date')))
df_cal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Pushing data to silver layer

# COMMAND ----------

# MAGIC %md
# MAGIC There are 4 modes available 
# MAGIC 1. append -- merging
# MAGIC 2. overwrite -- replace the data
# MAGIC 3. error -- cannot insert data
# MAGIC 4. ignore -- it wont write and throw error

# COMMAND ----------

df_cal.write.format('parquet')\
            .mode('append')\
            .option("path", "abfss://silver@asdatalake1.dfs.core.windows.net/AdventureWorks_Calendar")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation for customer file

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus = df_cus.withColumn('fullName',concat(col('Prefix'), lit(' '),col('FirstName'), lit(' ') ,col('LastName')))

# COMMAND ----------

df_cus = df_cus.withColumn('fullName', concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName')))
df_cus.display()



# COMMAND ----------

df_cus.write.format('parquet')\
            .mode('append')\
            .option("path", "abfss://silver@asdatalake1.dfs.core.windows.net/AdventureWorks_Customers")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### tf for subcategories

# COMMAND ----------

df_subcat.write.format('parquet')\
            .mode('append')\
            .option("path", "abfss://silver@asdatalake1.dfs.core.windows.net/Product_Subcategories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### tf for products

# COMMAND ----------

df_pro.display()

# COMMAND ----------

# MAGIC %md
# MAGIC productSKU - feth letter before - 
# MAGIC productname - first word before space.

# COMMAND ----------

df_pro = df_pro.withColumn('ProductSKU', split(col('ProductSKU'), '-')[0])\
                .withColumn('ProductName', split(col('ProductName'), ' ')[0])
df_pro.display()   

# COMMAND ----------

df_pro.write.format('parquet')\
            .mode('append')\
            .option("path", "abfss://silver@asdatalake1.dfs.core.windows.net/AdventureWork_Products")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Returns

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet')\
            .mode('append')\
            .option("path", "abfss://silver@asdatalake1.dfs.core.windows.net/AdventureWorks_Returns")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Territories

# COMMAND ----------

df_ter.write.format('parquet')\
            .mode('append')\
            .option("path", "abfss://silver@asdatalake1.dfs.core.windows.net/AdventureWorks_Territories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. stockdate - convert into timestamp
# MAGIC 2. ordernumber - convert 's' with 't'
# MAGIC 3. orderline * orderquantity

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate', to_timestamp(col('StockDate')))

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))

# COMMAND ----------

df_sales = df_sales.withColumn('Multiply', col('OrderLineItem')*col('OrderQuantity'))


# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Analysis 

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber')).alias('total_orders').display()


# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_sales.write.format('parquet')\
            .mode('append')\
            .option("path", "abfss://silver@asdatalake1.dfs.core.windows.net/AdventureWorks_Sales")\
            .save()

# COMMAND ----------

