# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS aqe_demo_db;
# MAGIC USE aqe_demo_db;
# MAGIC
# MAGIC DROP TABLE IF EXISTS items;
# MAGIC DROP TABLE IF EXISTS sales;
# MAGIC
# MAGIC -- Create "items" table.
# MAGIC
# MAGIC CREATE TABLE items
# MAGIC USING parquet
# MAGIC AS
# MAGIC SELECT id AS i_item_id,
# MAGIC CAST(rand() * 1000 AS INT) AS i_price
# MAGIC FROM RANGE(30000000);
# MAGIC
# MAGIC -- Create "sales" table with skew.
# MAGIC -- Item with id 100 is in 80% of all sales.
# MAGIC
# MAGIC CREATE TABLE sales
# MAGIC USING parquet
# MAGIC AS
# MAGIC SELECT CASE WHEN rand() < 0.8 THEN 100 ELSE CAST(rand() * 30000000 AS INT) END AS s_item_id,
# MAGIC CAST(rand() * 100 AS INT) AS s_quantity,
# MAGIC DATE_ADD(current_date(), - CAST(rand() * 360 AS INT)) AS s_date
# MAGIC FROM RANGE(1000000000);

# COMMAND ----------

salesDf = spark.read.table("aqe_demo_db.sales")
itemsDf = spark.read.table("aqe_demo_db.items")

# COMMAND ----------

spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", True)

# COMMAND ----------

import pyspark.sql.functions as F

# Dynamically Coalesce Shuffle Partitions

# The resulting data is very small, therefore the output partitions will be reduced dynamically based on the output of the transformation

coalescePartitionDf = salesDf.groupBy("s_date") \
    .agg(F.sum("s_quantity").alias("q")) \
    .orderBy(F.desc("q"))

# COMMAND ----------

coalescePartitionDf.show()

# COMMAND ----------

coalescePartitionDf.explain(True)

# COMMAND ----------

# Dynamically Switch Join Strategies

# Get the total sales amount grouped by sales date for items with a price lower than 10
# The selectivity of the filter by price is not shown in static planning, so the initial plan opts for sort merge join
# But in fact, the "items" table after filtering is very small, so the query can do a broadcast hash join instead
# Static explain shows the initial plan with sort merge join

switchJoinDf = salesDf.join(itemsDf, F.col("s_item_id") == F.col("i_item_id")) \
    .where("i_price = 10") \
    .groupBy("s_date") \
    .agg(F.sum(F.col("s_quantity") * F.col("i_price")).alias("total_sales")) \
    .orderBy(F.desc("total_sales"))

# COMMAND ----------

# Dynamically Optimize Skew Join

# Get the total sales amount grouped by sales date
# The partition in the "sales" table containing value "100" as "s_item_id" is much larger than other partitions
# AQE splits the skewed partition into smaller partitions before joining the "sales" table with the "items" table

skewJoinDf = salesDf.join(itemsDf, F.col("s_item_id") == F.col("i_item_id")) \
    .groupBy(F.col("s_date")) \
    .agg(F.sum(F.col("s_quantity") * F.col("i_price")).alias("total_sales")) \
    .orderBy(F.desc("total_sales"))

# COMMAND ----------


