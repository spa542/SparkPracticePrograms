// Databricks notebook source
// MAGIC %run ./Create_DataFrames

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val customerWithAddress = customerDf
.na.drop("any")
.join(addressDf, customerDf("address_id") === addressDf("address_id"))
.select('customer_id, 
        'demographics,
        concat_ws(" ",'firstname, 'lastname).as("Name"),
        addressDf("*")
       )

// COMMAND ----------

val salesWithItem = webSalesDf
.na.drop("any")
.join(itemDf,$"i_item_sk" === $"ws_item_sk")
.select(
            $"ws_bill_customer_sk".as("customer_id"),
            $"ws_ship_addr_sk".as("ship_address_id"),
            $"i_product_name".as("item_name"),
            trim($"i_category").as("item_category"),
            $"ws_quantity".as("quantity"),
            $"ws_net_paid".as("net_paid")
           )


// COMMAND ----------

val customerPurchases = salesWithItem
.join(customerWithAddress, salesWithItem("customer_id") === customerWithAddress("customer_id"))
.select(customerWithAddress("*"),
        salesWithItem("*"))
.drop(salesWithItem("customer_id"))

// COMMAND ----------

display(customerPurchases)

// COMMAND ----------



// COMMAND ----------

customerPurchases
.select($"*",
       trim($"item_category").as("it"))
.drop("item_category")
.withColumnRenamed("it","item_category")
.repartition(8)
.write
.partitionBy("item_category")
.option("compression","lz4")
.mode(SaveMode.Overwrite)
.option("path","tmp/output/customerPurchases")
.save

// COMMAND ----------

customerPurchases.select(length($"item_category")).show

// COMMAND ----------

// MAGIC %fs ls /tmp/output/customerPurchases/item_category=Books/

// COMMAND ----------

// MAGIC %fs ls tmp/output/customerPurchases

// COMMAND ----------

spark.read.parquet("dbfs:/tmp/output/customerPurchases/item_category=Books")
