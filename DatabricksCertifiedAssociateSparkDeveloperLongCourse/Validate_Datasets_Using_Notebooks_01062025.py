# Databricks notebook source
# MAGIC %fs ls /public/retail_db

# COMMAND ----------

# MAGIC %fs ls /public/retail_db/orders

# COMMAND ----------

dbutils.fs.ls('/public/retail_db/orders')

# COMMAND ----------

for file_details in dbutils.fs.ls('/public/retail_db/'):
    print(file_details)

# COMMAND ----------

for file_details in dbutils.fs.ls('/public/retail_db/'):
    print(file_details.path)

# COMMAND ----------


