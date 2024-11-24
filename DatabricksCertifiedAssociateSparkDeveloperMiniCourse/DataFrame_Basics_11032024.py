# Databricks notebook source
# Can use helper methods (more verbose)
customerDF = spark.read.json("dbfs:/FileStore/tables/dcad_data/customer.json")
# Can use format and load for reading data
#customerDF = spark.read.format("json").load("dbfs:/FileStore/tables/dcad_data/customer.json")


customerDF.printSchema()

# COMMAND ----------

customerDF.show(4)

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/dcad_data

# COMMAND ----------

# 1. Using a Schema String

# Creating a schema definition
customerDFSchema = "address_id INT, birth_country String, birthdate date, customer_id INT, demographics STRUCT<buy_potential:STRING, credit_rating: STRING, education_status: STRING, income_range: ARRAY<INT>, purchase_estimate: INT, vehicle_count: INT>, email_address STRING, firstname STRING, gender STRING, is_preffered_customer STRING, lastname STRING, salutation STRING"

# COMMAND ----------

# Apply the schema
customerDFwSchema = spark.read.schema(customerDfSchema).json("dbfs:/FileStore/tables/dcad_data/customer.json")

customerDFwSchema.printSchema()

# COMMAND ----------

# View the schema
customerDFwSchema.schema

# COMMAND ----------

# 2. Schema Specification w/ ST

from pyspark.sql.types import *

# Define the Schema as a structtype using built in functions
customerDFSchema_ST = StructType(
    [ # Use list here instead of Array(...) in Scala
        StructField("address_id", IntegerType(), True),
        StructField("birth_country", DateType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("demographics",
                    StructType([
                        StructField("buy_potential", StringType(), True),
                        StructField("credit_rating", StringType(), True),
                        StructField("education_status", StringType(), True),
                        StructField("income_range", ArrayType(IntegerType(), True), True),
                        StructField("purchase_estimate", IntegerType(), True),
                        StructField("vehicle_count", IntegerType(), True)
                    ]), True),
        StructField("email_address", StringType(), True),
        StructField("firstname", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("is_preffered_customer", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("salutation", StringType(), True)
    ]
)

# COMMAND ----------

# Apply the other schema
customerDFwSchema_ST = spark.read.schema(customerDFSchema_ST).json("dbfs:/FileStore/tables/dcad_data/customer.json")

customerDFwSchema_ST.printSchema()

# COMMAND ----------

# 3. Infer Schema
# Not good for production codebases. Costs extra compute and could result in unexpected errors in definition.
customerDFwSchema_ST = spark.read.option("inferSchema", True).json("dbfs:/FileStore/tables/dcad_data/customer.json")

customerDFwSchema_ST.printSchema()

# COMMAND ----------

help(customerDF.printSchema)

# COMMAND ----------


