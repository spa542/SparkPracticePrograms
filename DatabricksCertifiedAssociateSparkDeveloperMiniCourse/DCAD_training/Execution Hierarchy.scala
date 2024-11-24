// Databricks notebook source
// MAGIC %run ./Create_DataFrames

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val df = res49
.join(addressDf, 
      customerDf("address_id") === addressDf("address_id"))
.filter(year($"birthdate") > 1980)
.groupBy(customerDf("demographics.education_status")
         ,addressDf("location_type"))
.agg(countDistinct("customer_id").as("count"))
.where("location_type is not null")
.sort(desc("count"))



// COMMAND ----------

df.explain("formatted")

// COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",50)

// COMMAND ----------

customerDf.rdd.getNumPartitions

// COMMAND ----------

addressDf.rdd.getNumPartitions

// COMMAND ----------

df.show

// COMMAND ----------

df.explain("codegen")

// COMMAND ----------

df.explain(true)

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",10)

// COMMAND ----------

customerDf.rdd.getNumPartitions

// COMMAND ----------

customerDf.repartition(2)

// COMMAND ----------

customerDf.coalesce(2)

// COMMAND ----------

df.show

// COMMAND ----------

df.show
