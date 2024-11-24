# Databricks notebook source
# MAGIC %run ./DCAD_Training/Create_DataFrames_pyspark

# COMMAND ----------

# Sample DFs
import pyspark.sql.functions as F

customerWithAddress = customerDf \
  .na.drop("any") \
  .join(addressDf, on="address_id") \
  .select("customer_id", "demographics", F.concat_ws(" ", F.col("firstname"), F.col("lastname")).alias("Name"), addressDf["*"])


salesWithItem = webSalesDf \
  .na.drop("any") \
  .join(itemDf, on=(F.col("i_item_sk") == F.col("ws_item_sk"))) \
  .selectExpr("ws_bill_customer_sk customer_id",
              "ws_ship_addr_sk ship_address_id",
              "i_product_name item_name",
              "i_category item_category",
              "ws_quantity quantity",
              "ws_net_paid net_paid")
  
customerPurchases = salesWithItem \
  .join(customerWithAddress, on=(salesWithItem["customer_id"] == customerWithAddress["customer_id"])) \
  .select(customerWithAddress["*"], salesWithItem["*"]) \
  .drop(salesWithItem["customer_id"])

# COMMAND ----------

# DataFrameWriter API
customerPurchases.show()

# COMMAND ----------

customerPurchases \
  .write \
  .option("path", "/tmp/output/customerPurchases") \ # Will create all the folders and files for us repsectively
  .save()

# COMMAND ----------

# MAGIC %fs ls /tmp/output/customerPurchases

# COMMAND ----------

customerPurchases.rdd.getNumPartitions()

# COMMAND ----------

customerPurchases \
  .repartition(1) \
  .write \
  .mode("overwrite") \
  .option("path", "/tmp/output/customerPurchasesRepartition") \
  .save()

# COMMAND ----------

# MAGIC %fs ls /tmp/output/customerPurchasesRepartition

# COMMAND ----------

customerPurchases \
  .repartition(8) \
  .write \
  .option("compression", "lz4") \
  .format("json") \
  .mode("overwrite") \
  .option("path", "/tmp/output/customerPurchasesJSON") \
  .save()

# COMMAND ----------

# MAGIC %fs ls /tmp/output/customerPurchasesJSON

# COMMAND ----------

# DataFrame Writer PartitionBy
customerPurchases \
  .repartition(8) \
  .write \
  .partitionBy("item_category") \
  .option("compression", "snappy") \
  .format("json") \
  .mode("overwrite") \
  .option("path", "/tmp/output/customerPurchasesPartitionBy") \
  .save()

# COMMAND ----------

# MAGIC %fs ls /tmp/output/customerPurchasesPartitionBy

# COMMAND ----------

# Read data from a particular partition
partitionRead = spark.read.json("dbfs:/tmp/output/customerPurchasesPartitionBy/item_category=Books                                             /")

partitionRead.show()

# COMMAND ----------


