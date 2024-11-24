// Databricks notebook source
// MAGIC %run ./Create_DataFrames

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

customerDf.select("firstname","lastname","demographics","demographics.credit_rating")

// COMMAND ----------

import org.apache.spark.sql.functions.{col,expr,column}

// COMMAND ----------

customerDf.select($"firstname",'lastname,col("demographics.education_status"),
                 column("demographics.credit_rating"),
                 customerDf.col("salutation"),
                 expr("concat(firstname,lastname) name"))

// COMMAND ----------

customerDf.select("firstname",$"lastname")

// COMMAND ----------

customerDf.selectExpr("birthdate birthday", "year(birthdate) birthyear")

// COMMAND ----------

customerDf.columns

// COMMAND ----------

customerDf.withColumnRenamed($"email_address","mail")

// COMMAND ----------

customerDf.printSchema

// COMMAND ----------

customerDf.printSchema

// COMMAND ----------

import org.apache.spark.sql.types._

// COMMAND ----------

customerDf.select($"address_id".cast("long"),$"birthdate".cast(StringType))

// COMMAND ----------

customerDf.selectExpr("cast(address_id as string)","cast(demographics.income_range[0] as double) lowerBound")

// COMMAND ----------

display(customerDf.select("address_id","demographics.income_range"))

// COMMAND ----------

customerDf.columns.size

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

customerDf.withColumn("fullname",expr("address_id + 1"))

// COMMAND ----------

customerDf.columns

// COMMAND ----------

customerDf.drop("address_id","birth_country","lkajkdjf").columns

// COMMAND ----------

customerDf.drop($"lastname",$"firstname").columns

// COMMAND ----------

val salesStatDf = webSalesDf.select(
"ws_order_number",
"ws_item_sk",
  "ws_quantity",
  "ws_net_paid",
  "ws_net_profit",
  "ws_wholesale_cost"
)

// COMMAND ----------

val salesPerfDf = salesStatDf
.withColumn("expected_net_paid", $"ws_quantity" * $"ws_wholesale_cost")
.withColumn("calculated_profit", $"ws_net_paid" - $"expected_net_paid")
.withColumn("unit_price",expr("ws_wholesale_cost / ws_quantity"))
.withColumn("rounded_unitPrice",round($"unit_price",2))
.withColumn("brounded_unitPrice",bround($"unit_price",3))

// COMMAND ----------

display(salesPerfDf)

// COMMAND ----------

val Df = customerDf
.withColumnRenamed("email_address","mail")
.withColumn("developer_site",lit("insightahead.com"))
.drop("birth_country","address_id")
.filter(dayofmonth($"birthdate") < 20)

// COMMAND ----------

customerDf.explain("formatted")

// COMMAND ----------

Df.explain("formatted")

// COMMAND ----------

val filtered = customerDf
.where(year($"birthdate") > 1980)
.filter(month($"birthdate") === 1 )
.where("day(birthdate) > 15")
.filter($"birth_country" =!= "UNITED STATES" )
.select("firstname","lastname","birthdate","birth_country")

// COMMAND ----------

filtered.show

// COMMAND ----------

val demoDf = List(
  (1,"Monday"), (1,"Monday"),
  (2,"Tuesday"),  (3,"Tuesday"),
  (3,"Wednesday"), (4,"Thursday"),
  (5,"Friday"),  (6,"Saturday"),
  (7,"Sunday")).toDF("id","name")

// COMMAND ----------

demoDf.show

// COMMAND ----------

demoDf.distinct.show

// COMMAND ----------

demoDf.dropDuplicates("name").show

// COMMAND ----------

demoDf.dropDuplicates(List("name","id")).show

// COMMAND ----------

demoDf.distinct.show

// COMMAND ----------

demoDf.dropDuplicates.show

// COMMAND ----------

val Dfn = customerDf.selectExpr("salutation","firstname","lastname","email_address","year(birthdate) birthyear ")

// COMMAND ----------

display(Dfn)

// COMMAND ----------

Dfn.count

// COMMAND ----------

Dfn.where($"salutation".isNotNull).count

// COMMAND ----------

Dfn.where($"salutation" === null).count

// COMMAND ----------

Dfn.where($"salutation".isNull).count

// COMMAND ----------

display(Dfn.where($"salutation".isNull))

// COMMAND ----------

Dfn.na.drop(how="all").count

// COMMAND ----------

display(Dfn.na.drop(how="all"))

// COMMAND ----------

display(Dfn.na.drop("any"))

// COMMAND ----------

display(Dfn.na.drop("any",Seq("firstname","lastname")))

// COMMAND ----------

display(Dfn.na.fill(1234))

// COMMAND ----------

display(Dfn.na.fill(Map("salutation" -> "UNKNOW","firstname" -> "John", "lastname" -> "Doe", "birthyear" -> 9999)))

// COMMAND ----------

display(customerDf
.na.drop("any")
.sort($"firstname",$"lastname".desc)
.select("firstname","lastname","birthdate"))

// COMMAND ----------

webSalesDf.printSchema

// COMMAND ----------

val customerPurchases = webSalesDf.selectExpr("ws_bill_customer_sk customer_id","ws_item_sk item_id")

// COMMAND ----------

customerPurchases.show

// COMMAND ----------

customerPurchases.groupBy("customer_id").agg(count("item_id").alias("item_count")).show

// COMMAND ----------

customerDf.select(year($"birthdate")).show

// COMMAND ----------

webSalesDf.agg(
max("ws_sales_price"),
min("ws_sales_price"),
avg("ws_sales_price"),
mean("ws_sales_price"),
count("ws_sales_price")
)
.show

// COMMAND ----------

webSalesDf.count

// COMMAND ----------

webSalesDf.select(count("*")).show

// COMMAND ----------

webSalesDf.select(count("ws_sales_price")).show

// COMMAND ----------

display(webSalesDf.describe("ws_sales_price"))

// COMMAND ----------

display(webSalesDf.summary("stddev"))

// COMMAND ----------

display(customerDf.groupBy(
  customerDf("birth_country"), $"birthdate")
        .agg(count("*").as("count"))
       .sort(desc("count")))

// COMMAND ----------

display(addressDf)

// COMMAND ----------

display(customerDf)

// COMMAND ----------

val customerWithAddress =
customerDf.join(addressDf, customerDf.col("address_id") === addressDf.col("address_id"),"inner")
.select("customer_id"
       ,"firstname"
       ,"lastname"
       ,"demographics.education_status"
       ,"location_type"
       ,"country"
       ,"city"
       ,"street_name"
      )

// COMMAND ----------

display(customerWithAddress)

// COMMAND ----------

webSalesDf.select(countDistinct("ws_item_sk")).show

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/dcad_data

// COMMAND ----------

webSalesDf

// COMMAND ----------

webSalesDf.where("ws_bill_customer_sk = 45721").count

// COMMAND ----------

// display(
webSalesDf.join(customerDf, $"customer_id" === $"ws_bill_customer_sk","left")
.select("customer_id","ws_bill_customer_sk","*")
.where("customer_id is null")
  .count
//   )

// COMMAND ----------

webSalesDf.where("ws_bill_customer_sk is null").count

// COMMAND ----------

customerDf.select("firstname","lastname","customer_id")

// COMMAND ----------

val df1 = customerDf
.select("firstname","lastname","customer_id")
// .withColumn("from",lit("df1"))

// COMMAND ----------

val df2 = customerDf
.select("lastname","firstname","customer_id")
// .withColumn("from",lit("df2"))

// COMMAND ----------

df1.unionByName(df2).distinct.where("customer_id = 45721").show

// COMMAND ----------

val customerWithAddr = customerDf
.join(addressDf, customerDf("address_id") === addressDf("address_id"))

// COMMAND ----------

customerWithAddr.count

// COMMAND ----------

customerWithAddr.cache.count

// COMMAND ----------

customerWithAddr.count

// COMMAND ----------

customerWithAddr.unpersist.count

// COMMAND ----------

import org.apache.spark.storage.StorageLevel

// COMMAND ----------

customerWithAddr.persist(StorageLevel.MEMORY_AND_DISK).count

// COMMAND ----------

customerWithAddr.count

// COMMAND ----------

customerWithAddr.storageLevel

// COMMAND ----------

customerWithAddr.unpersist.count

// COMMAND ----------

customerWithAddr.persist(StorageLevel.MEMORY_ONLY_SER_2).count

// COMMAND ----------

customerWithAddr.storageLevel

// COMMAND ----------

customerWithAddr.storageLevel

// COMMAND ----------

customerWithAddr.show
