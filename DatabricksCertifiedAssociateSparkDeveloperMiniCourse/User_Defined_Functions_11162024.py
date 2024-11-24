# Databricks notebook source
# MAGIC %run ./DCAD_Training/Create_DataFrames_pyspark

# COMMAND ----------

# User Defined Functions
import pyspark.sql.functions as F
from pyspark.sql.types import *

def string_concat(sep, first, second):
  return f"{first}{sep}{second}"

# COMMAND ----------

# Register the Function
string_concat_udf = udf(string_concat, StringType()) # In Scala, need to define the input and output parameters and types, Python only needs return value as a pyspark type

# COMMAND ----------

# Utilize the function
customerDf.select("firstname", "lastname", string_concat_udf(F.lit("-"), F.col("firstname"), F.col("lastname")).alias("full_name")).show()

# COMMAND ----------

# Register the function as an SQL function
spark.udf.register("string_concat_udf_sql", string_concat, StringType())

# COMMAND ----------

customerDf.selectExpr("firstname", "lastname", "string_concat_udf_sql('->',firstname,lastname) as full_name_arrow").show()

# COMMAND ----------

# Same DF as a SQL table
customerDf.write.saveAsTable("customer")

# COMMAND ----------

spark.sql("select firstname, lastname from customer").show()

# COMMAND ----------


