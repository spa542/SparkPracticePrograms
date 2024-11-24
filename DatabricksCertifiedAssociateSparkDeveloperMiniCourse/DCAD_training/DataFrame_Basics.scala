// Databricks notebook source
val customerDf = spark.read.format("json").load("/FileStore/tables/dcad_data/customer.json")

// COMMAND ----------

customerDf.printSchema()

// COMMAND ----------

val customerDf = spark.read.format("json")
.option("inferSchema",true)
.schema(customerDfSchema_ST)
// .schema(customerDfSchema_DDL)
.load("/FileStore/tables/dcad_data/customer.json")

// COMMAND ----------

val customerDfSchema_DDL = "address_id INT,birth_country STRING,birthdate date,customer_id INT,demographics STRUCT<buy_potential: STRING, credit_rating: STRING, education_status: STRING, income_range: ARRAY<INT>, purchase_estimate: INT, vehicle_count: INT>,email_address STRING,firstname STRING,gender STRING,is_preffered_customer STRING,lastname STRING,salutation STRING"

// COMMAND ----------

import org.apache.spark.sql.types._

// COMMAND ----------

val customerDfSchema_ST = StructType(
Array(
StructField("address_id",IntegerType,true),
                   StructField("birth_country",StringType,true), 
                   StructField("birthdate",DateType,true), 
                   StructField("customer_id",IntegerType,true),
                   StructField("demographics",
                               StructType(Array(
                                                StructField("buy_potential",StringType,true),
                                                StructField("credit_rating",StringType,true), 
                                                StructField("education_status",StringType,true), 
                                                StructField("income_range",ArrayType(IntegerType,true),true), 
                                                StructField("purchase_estimate",IntegerType,true), 
                                                StructField("vehicle_count",IntegerType,true))
                                         ),true),  
                   StructField("email_address",StringType,true),
                   StructField("firstname",StringType,true),
                   StructField("gender",StringType,true), 
                   StructField("is_preffered_customer",StringType,true),
                   StructField("lastname",StringType,true),
                   StructField("salutation",StringType,true)
))


// COMMAND ----------

customerDf.printSchema

// COMMAND ----------

customerDf.schema

// COMMAND ----------

customerDf.printSchema

// COMMAND ----------

customerDf.printSchema()

// COMMAND ----------

display(customerDf)

// COMMAND ----------

// MAGIC %fs head /FileStore/tables/dcad_data/customer.json

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/dcad_data
