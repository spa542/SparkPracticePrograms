# Databricks notebook source
# MAGIC %run ./DCAD_Training/Create_DataFrames_pyspark

# COMMAND ----------

# Select Examples
# Can select using a dot operation for sub arrays and struct types
result = customerDf.select("firstname", "lastname", "demographics", "demographics.credit_rating")

# COMMAND ----------

result.show(2)

# COMMAND ----------

# Select using Column Objects (Many different ways)
# Note: In Scala you can only use Column objects OR strings, not both... unlike Pyspark!!!
import pyspark.sql.functions as F

result2 = customerDf.select(f"firstname", F.col("demographics.education_status"), F.column("demographics.credit_rating"), customerDf.salutation, F.expr("concat(firstname,lastname) name"))

# COMMAND ----------

result2.show(2)

# COMMAND ----------

# Another Select Expression (Allows you to use some sql expressions)
customerDf.selectExpr("birthdate birthday", "year(birthdate)").show(2)

# COMMAND ----------

# Renaming Columns
customerDf.columns

# COMMAND ----------

# Rename email_address to mail column name
# Note: If you specify a column name that DNE, Spark will do nothing!!!
# However, if you specify a column object that DNE, it will fail!
customerDf.withColumnRenamed("email_address", "mail").columns

# COMMAND ----------

# Examples from above
customerDf.withColumnRenamed("asjdflkasjd", "hi").columns
#customerDf.withColumnRenamed(F.col("aksdjlfask"), "hi").columns # Will Fail

# COMMAND ----------

# Change the Data Type for Columns
customerDf.printSchema()

# COMMAND ----------

# Change datatype using different methods
from pyspark.sql.types import *

customerDf.select(F.col("address_id").cast("long"), F.col("birthdate").cast(StringType())).printSchema()

customerDf.selectExpr("cast(address_id as string)", "cast(demographics.income_range[0] as double) lowerBound").printSchema()

# COMMAND ----------

# Adding columns to a DataFrame
len(customerDf.columns)

# COMMAND ----------

# Using withColumn
len(customerDf.withColumn("developer_site", F.lit("insightahead.com")).columns)

# COMMAND ----------

# Another Example
len(customerDf.withColumn("fullname", F.concat("firstname", "lastname")).columns)
len(customerDf.withColumn("fullname", F.concat_ws(" ", "firstname", "lastname")).columns)
len(customerDf.withColumn("fullname", F.expr("address_id + 1")).columns)

# COMMAND ----------

# Removing Columns from a DataFrame
customerDf.columns

# COMMAND ----------

# Example
# Same edge case as before. If string column provided does not exist, Spark will do nothing. Otherwise, column objects will fail.
customerDf.drop("address_id", "birth_country").columns

# COMMAND ----------

# Basic Arithmetic
salesStatDF = webSalesDf.select("ws_order_number", "ws_item_sk", "ws_quantity", "ws_net_paid", "ws_net_profit", "ws_wholesale_cost")

# COMMAND ----------

# Example
# You can do all sorts of operations
# Can even use a wide variety of built in function from spark like rounding and exponents
salesPerfDf = salesStatDF \
    .withColumn("expected_net_paid", F.col("ws_quantity") * F.col("ws_wholesale_cost")) \
    .withColumn("unit_price", F.expr("ws_wholesale_cost / ws_quantity"))

salesPerfDf.show(2)

# COMMAND ----------

# Get the lineage of how the dataframe was computed
salesPerfDf.explain("formatted")

# COMMAND ----------

# Filtering a DataFrame
# Various examples
customerDf.where(F.year(F.col("birthdate")) > 1980) \
    .filter(F.month(F.col("birthdate")) == 1) \
    .where("day(birthdate) > 15") \
    .filter(F.col("birth_country") != "UNITED STATES") \
    .select("firstname", "lastname", "birthdate", "birth_country").show(5)

# COMMAND ----------

# Dropping Rows
dummy_df = spark.createDataFrame(
    [
        (1, "Monday"),
        (2, "Tuesday"),
        (3, "Wednesday"),
        (5, "Friday"),
        (7, "Sonnday"),
        (1, "Monday"),
        (3, "Tuesday"),
        (4, "Thursday"),
        (6, "Saturday")
    ]
)

dummy_df.show()

# COMMAND ----------

# Get unique rows
dummy_df.distinct().show()

# COMMAND ----------

# Drop duplicates function
dummy_df.dropDuplicates(["_2"]).show()
dummy_df.dropDuplicates(["_1", "_2"]).show()
dummy_df.dropDuplicates().show() # Has the same functionality of the distinct method

# COMMAND ----------

# Handling Null Values - Null Functions
Dfn = customerDf.selectExpr("salutation", "firstname", "lastname", "email_address", "year(birthdate) birthyear")

Dfn.show()

# COMMAND ----------

Dfn.count()

# COMMAND ----------

# Can use the isNotNull method
print(Dfn.where(F.col("salutation").isNotNull()).count())
print(Dfn.filter(F.col("salutation").isNotNull()).count())

# COMMAND ----------

# Can use NA methods
print(Dfn.na.drop(how="all").count())
print(Dfn.na.drop(how="any").count())

# COMMAND ----------

# Can specify when to remove a record with a specified subset
Dfn.na.drop(subset=["firstname", "lastname"]).count() # This will look a lot different from the Scala interpretation

# COMMAND ----------

# Can fill null values
Dfn.na.fill("insightahead").show() # Will update only type String

# COMMAND ----------

# Fill with a map
Dfn.na.fill({"salutation": "UNKNOWN", "firstname": "John", "lastname": "Doe", "birthyear": 9999}).show()

# COMMAND ----------

# Sort Rows in the DataFrame
customerDf.na.drop("any").sort("firstname").select("firstname", "lastname", "birthdate").show() # Default is ascending order
customerDf.na.drop("any").sort(F.expr("firstname")).select("firstname", "lastname", "birthdate").show() # Can sort by multiple columns at a time by providing multiple columns. You can also sort by various orders in each
customerDf.na.drop("any").sort(F.expr("firstname")).orderBy(F.expr("month(birthdate)").desc()).select("firstname", "lastname", "birthdate").show()

# COMMAND ----------

# Grouping Data
webSalesDf.printSchema()

# COMMAND ----------

customerPurchases = webSalesDf.selectExpr("ws_bill_customer_sk customer_id", "ws_item_sk item_id")
customerPurchases.show()

# COMMAND ----------

# GroupBy followed by some aggregation
customerPurchases.groupBy("customer_id").agg(F.count("item_id").alias("item_count")).show()

# COMMAND ----------

# DataFrame Statistics and Manipulations
webSalesDf.select(F.max("ws_sales_price")).show() # Can use min, max, and many others

# COMMAND ----------

customerDf.select(F.year("birthdate")).show()

# COMMAND ----------

webSalesDf.agg(
    F.max("ws_sales_price"),
    F.min("ws_sales_price"),
    F.avg("ws_sales_price"),
    F.mean("ws_sales_price"),
    F.count("ws_sales_price")
).show() # Allows for multiple aggregations

# COMMAND ----------

# Count function in-depth
webSalesDf.count()

# COMMAND ----------

webSalesDf.select(F.count("*")).show()

# COMMAND ----------

webSalesDf.select(F.count("ws_sales_price")).show() # Will only count rows that have values for this column specified

# COMMAND ----------

webSalesDf.describe().show() # Get various standard metrics

# COMMAND ----------

webSalesDf.describe("ws_sales_price").show() # For specific columns

# COMMAND ----------

webSalesDf.summary().show() # Contains all standard metrics in a cleaner manner
#webSalesDf.summary("stddev").show() # Specify the metric

# COMMAND ----------

# Inner Joins
customerDf.join(addressDf, on="address_id", how="inner").show() # Inner join is implicit

# COMMAND ----------

# Right Outer Join
webSalesDf.join(customerDf, on=(F.col("ws_bill_customer_sk") == F.col("customer_id")), how="right").select("customer_id", "ws_bill_customer_sk") \
    .where(F.col("ws_bill_customer_sk").isNull()) \
    .withColumn("send_promo", F.col("customer_id") == F.col("ws_bill_customer_sk")) \
    .where(F.col("send_promo").isNull()).show() # Join can explicitly udnerstand the column names if they are unique between the two dataframes

# COMMAND ----------

# Left Outer Join
webSalesDf.join(customerDf, on=(F.col("ws_bill_customer_sk") == F.col("customer_id")), how="left") \
    .select("customer_id", "ws_bill_customer_sk", "*") \
    .show()

# COMMAND ----------

# Union - Adding rows to a dataframe
df1 = customerDf.select("firstname", "lastname", "customer_id").withColumn("from", F.lit("df1"))
df2 = customerDf.select("lastname", "firstname", "customer_id").withColumn("from", F.lit("df2"))

# NOTE: Will return duplicate rows if any
df1.union(df2).where("customer_id = 45721").show()
df1.unionByName(df2).where("customer_id = 45721").show() # Will Adjust and fix the reordering of rows as shown below

# COMMAND ----------

# Caching a Dataframe
customerWithAddr = customerDf.join(addressDf, on="address_id")

# COMMAND ----------

customerWithAddr.count()

# COMMAND ----------

customerWithAddr.cache().count()

# COMMAND ----------

customerWithAddr.count()

# COMMAND ----------

# Persist the same
customerWithAddr.persist().count()

# COMMAND ----------

from pyspark import StorageLevel
customerWithAddr.persist(StorageLevel.MEMORY_AND_DISK).count()

# COMMAND ----------

customerWithAddr.storageLevel

# COMMAND ----------

customerWithAddr.unpersist().count()

# COMMAND ----------


