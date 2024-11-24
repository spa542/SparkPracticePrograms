// Databricks notebook source
// MAGIC %md
// MAGIC https://databricks.com/de/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html

// COMMAND ----------

// MAGIC %md
// MAGIC https://docs.databricks.com/_static/notebooks/aqe-demo.html?_ga=2.96665439.1135627284.1602308202-183110992.1602308202

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS aqe_demo_db;
// MAGIC USE aqe_demo_db;
// MAGIC
// MAGIC DROP TABLE IF EXISTS items;
// MAGIC DROP TABLE IF EXISTS sales;
// MAGIC
// MAGIC -- Create "items" table.
// MAGIC
// MAGIC CREATE TABLE items
// MAGIC USING parquet
// MAGIC AS
// MAGIC SELECT id AS i_item_id,
// MAGIC CAST(rand() * 1000 AS INT) AS i_price
// MAGIC FROM RANGE(30000000);
// MAGIC
// MAGIC -- Create "sales" table with skew.
// MAGIC -- Item with id 100 is in 80% of all sales.
// MAGIC
// MAGIC CREATE TABLE sales
// MAGIC USING parquet
// MAGIC AS
// MAGIC SELECT CASE WHEN rand() < 0.8 THEN 100 ELSE CAST(rand() * 30000000 AS INT) END AS s_item_id,
// MAGIC CAST(rand() * 100 AS INT) AS s_quantity,
// MAGIC DATE_ADD(current_date(), - CAST(rand() * 360 AS INT)) AS s_date
// MAGIC FROM RANGE(1000000000);

// COMMAND ----------

val salesDf = spark.read.table("aqe_demo_db.sales")

// COMMAND ----------

val itemsDf = spark.read.table("aqe_demo_db.items")

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC # Enable Adaptive Query Execution (AQE)

// COMMAND ----------

spark.conf.get("spark.sql.adaptive.enabled")

// COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled","true")

// COMMAND ----------

// MAGIC %md 
// MAGIC # Dynamically Coalesce Shuffle Partitions
// MAGIC

// COMMAND ----------

val coalescePartitionDf =
salesDf.groupBy("s_date")
.agg(sum("s_quantity").as("q"))
.orderBy(desc("q"))


// COMMAND ----------

coalescePartitionDf.show

// COMMAND ----------

coalescePartitionDf.show

// COMMAND ----------

// MAGIC %md
// MAGIC https://spark.apache.org/docs/latest/sql-performance-tuning.html#coalescing-post-shuffle-partitions

// COMMAND ----------

// MAGIC %md
// MAGIC # Dynamically Switch Join Strategies

// COMMAND ----------

// Get total sales amount grouped by sales date for items with a price lower than 10.
// The selectivity of the filter by price is not known in static planning, so the initial plan opts for sort merge join.
// But in fact, the "items" table after filtering is very small, so the query can do a broadcast hash join instead.
// Static explain shows the initial plan with sort merge join.

val switchJoinDf = 
salesDf.join(itemsDf, 's_item_id === 'i_item_id)
.where("i_price < 10")
.groupBy('s_date)
.agg(sum('s_quantity * 'i_price).as("total_sales"))
.orderBy(desc("total_sales"))

// COMMAND ----------

// MAGIC %md
// MAGIC # Dynamically Optimize Skew Join

// COMMAND ----------

// Get the total sales amount grouped by sales date.
// The partition in the "sales" table containing value "100" as "s_item_id" is much larger than other partitions.
// AQE splits the skewed partition into smaller partitions before joining the "sales" table with the "items" table.

val skewJoinDf = 
salesDf.join(itemsDf, 's_item_id === 'i_item_id)
.groupBy('s_date)
.agg(sum('s_quantity * 'i_price).as("total_sales"))
.orderBy(desc("total_sales"))

// COMMAND ----------

switchJoinDf.show
