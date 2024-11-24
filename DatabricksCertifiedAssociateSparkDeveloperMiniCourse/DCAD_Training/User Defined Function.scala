// Databricks notebook source
// MAGIC %run ./Create_DataFrames

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

def stringConcat(sep:String,first:String,second:String): String = {
return first + sep + second
}

// COMMAND ----------

// MAGIC %md
// MAGIC Register the function as DataFrame function

// COMMAND ----------

val stringConcat_udf = udf(stringConcat(_:String,_:String,_:String):String)

// COMMAND ----------

customerDf.select($"firstname",$"lastname",stringConcat_udf(lit(" "),'firstname,'lastname).as("fullName")).show

// COMMAND ----------

// MAGIC %md
// MAGIC register the function as SQL function

// COMMAND ----------

spark.udf.register("SQL_StringConcat_udf",stringConcat(_:String,_:String,_:String):String)

// COMMAND ----------

customerDf.selectExpr("firstname","lastname","SQL_StringConcat_udf('->',firstname,lastname)").show

// COMMAND ----------

customerDf.write.saveAsTable("customer")

// COMMAND ----------

spark.sql("select firstname, lastname,stringConcat_udf(' ',firstname,lastname) as fullName from customer").show
