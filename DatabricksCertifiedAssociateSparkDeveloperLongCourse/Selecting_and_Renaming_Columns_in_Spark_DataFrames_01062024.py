# Databricks notebook source
# Creating Spark DataFrame to Select and Rename Columns

from pyspark.sql import Row
import datetime

users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Brown",
        "email": "test@etsy.com",
        "phone_numbers": Row(mobile="1234567890", home="0987654321"),
        "courses": [1, 2],
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_udpated_ts": datetime.datetime(2021, 1, 15, 12, 30, 0)
    },
    {
        "id": 2,
        "first_name": "Yaga",
        "last_name": "Nara",
        "email": "ahh@etsy.com",
        "phone_numbers": Row(mobile=None, home=None), # Will get converted into a struct
        "courses": [3, 4], # Will get converted into an array
        "is_customer": True,
        "amount_paid": None,
        "customer_from": datetime.date(2021, 1, 25),
        "last_udpated_ts": datetime.datetime(2021, 1, 25, 12, 30, 0)
    }
]

# COMMAND ----------

spark.conf.set('spark.sql.exectuion.arrow.pyspark.enabled', False)

# COMMAND ----------

import pandas as pd

users_df = spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

users_df.show()

# COMMAND ----------

help(users_df.select)

# COMMAND ----------

users_df.select('*').show()

# COMMAND ----------

users_df.select('id', 'first_name', 'last_name').show()

# COMMAND ----------

users_df.select(['id', 'first_name', 'last_name']).show()

# COMMAND ----------

users_df.alias('u').select('u.*').show()

# COMMAND ----------

users_df.alias('u').select('u.id', 'u.first_name', 'u.last_name').show()

# COMMAND ----------

import pyspark.sql.functions as F

users_df.select(F.col('id'), 'first_name', 'last_name').show()

# COMMAND ----------

users_df.select(
    F.col('id'),
    'first_name',
    'last_name',
    F.concat(F.col('first_name'), F.lit(', '), F.col('last_name')).alias('full_name')
).show()

# COMMAND ----------

help(users_df.selectExpr)

# COMMAND ----------

users_df.selectExpr('*').show()

# COMMAND ----------

users_df.selectExpr('id', 'first_name', 'last_name').show()

# COMMAND ----------

users_df.selectExpr(['id', 'first_name', 'last_name']).show()

# COMMAND ----------

users_df.select('id', 'first_name', 'last_name', F.concat(F.col('first_name'), F.lit(' '), F.col('last_name')).alias('full_name')).show()

# COMMAND ----------

users_df.selectExpr('id', 'first_name', 'last_name', 'concat(first_name, ", ", last_name) AS full_name').show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
    SELECT id, first_name, last_name, concat(first_name, ', ', last_name) AS full_name FROM users      
""").show()

# COMMAND ----------

users_df['id']

# COMMAND ----------

type(users_df['id'])

# COMMAND ----------

users_df.select('id', F.col('first_name'), 'last_name').show()

# COMMAND ----------

users_df.select(users_df['id'], F.col('first_name'), 'last_name').show()

# COMMAND ----------

users_df.alias('u').select(u['id'], F.col('first_name'), 'last_name').show() # THIS WILL NOT WORK - alias can only be used in strings

# COMMAND ----------

users_df.alias('u').select('u.id', F.col('first_name'), 'last_name').show()

# COMMAND ----------

users_df.selectExpr(F.col('id'), 'first_name', 'last_name').show() # THIS WILL NOT WORK - we cannot pass column type objects to selectExpr (only strings!)

# COMMAND ----------

users_df.select(
    'id', 'first_name', 'last_name',
    F.concat(users_df['first_name'], F.lit(', '), F.col('last_name')).alias('full_name')
).show()

# COMMAND ----------

users_df.alias('u').selectExpr('id', 'first_name', 'last_name', "concat(u.first_name, ', ', u.last_name) AS full_name").show()

# COMMAND ----------

# SQL Equivalent
users_df.createGlobalTempView('users')

# COMMAND ----------

spark.sql("""
        SELECT id, first_name, last_name,
        concat(u.first_name, ', ', u.last_name) AS full_name
        FROM users as u          
""").show()

# COMMAND ----------

users_df['id']

# COMMAND ----------

F.col('id')

# COMMAND ----------

users_df.select('id', 'first_name', 'last_name').show()

# COMMAND ----------

cols = ['id', 'first_name', 'last_name']
users_df.select(*cols).show()

# COMMAND ----------

help(F.col)

# COMMAND ----------

user_id = F.col('id')

# COMMAND ----------

user_id

# COMMAND ----------

users_df.select(user_id).show()

# COMMAND ----------

users_df.select(
    F.col('id'),
    F.date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')
).show()

# COMMAND ----------

users_df.select(
    F.col('id'),
    F.date_format('customer_from', 'yyyyMMdd')
).printSchema()

# COMMAND ----------

users_df.select(
    F.col('id'),
    F.date_format('customer_from', 'yyyyMMdd').alias('customer_from')
).show()

# COMMAND ----------

cols = [F.col('id'), F.date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')]

users_df.select(*cols).show()

# COMMAND ----------

full_name_col = F.concat(F.col('first_name'), F.lit(', '), F.col('last_name')).alias('full_name')

# COMMAND ----------

full_name_col

# COMMAND ----------

users_df.select('id', full_name_col).show()

# COMMAND ----------

users_df.select('id', 'customer_from').show()

# COMMAND ----------

customer_from_col = F.date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')

customer_from_col

# COMMAND ----------

users_df.select('id', customer_from_col).show()

# COMMAND ----------

# Understanding the lit function
#users_df.select('id', 'amount_paid' + 25).show() # This will not work! As it this is a normal literal -> not a column type literal
users_df.select('id', F.col('amount_paid') + F.lit(25)).show() # Must use a column object here or will get NULL due to amount_paid being treated as literal string in addition expression

# COMMAND ----------

users_df.createOrReplaceTempView('users')

spark.sql("""
        SELECT id, (amount_paid + 25) AS amount_paid FROM users
""").show()

# COMMAND ----------

users_df.selectExpr('id', 'amount_paid + 25 AS amount_paid').show()

# COMMAND ----------

F.lit(25)

# COMMAND ----------

# Using withColumn

users_df.select(
    'id', 'first_name', 'last_name',
    F.concat('first_name', F.lit(', '), 'last_name').alias('full_name')
).show()

# COMMAND ----------

users_df.select(
    'id', 'first_name', 'last_name') \
    .withColumn('full_name', F.concat('first_name', F.lit(', '), 'last_name')) \
.show()

# COMMAND ----------

help(users_df.withColumn)

# COMMAND ----------

# Needs to be a column object to avoid confusion with withColumn calculations
users_df. \
    select('id', 'first_name', 'last_name'). \
    withColumn('fn', F.col('first_name')) \
    .show()

# COMMAND ----------

users_df.select('id', 'courses').show()

# COMMAND ----------

users_df.select('id', 'courses').dtypes

# COMMAND ----------

users_df.select('id', 'courses') \
    .withColumn('course_count', F.size('courses')) \
    .show()

# COMMAND ----------

help(users_df.withColumnRenamed)

# COMMAND ----------

users_df. \
    select('id', 'first_name', 'last_name'). \
        show()

# COMMAND ----------

users_df. \
    select('id', 'first_name', 'last_name'). \
    withColumnRenamed('id', 'user_id'). \
    withColumnRenamed('first_name', 'user_first_name'). \
    withColumnRenamed('last_name', 'user_last_name'). \
    show()

# COMMAND ----------

user_id = F.col('id')

# COMMAND ----------

help(user_id.alias)

# COMMAND ----------

# Using Select
users_df \
    .select(
        F.col('id').alias('user_id'),
        F.col('first_name').alias('user_first_name'),
        F.col('last_name').alias('user_last_name'),
        F.concat(F.col('first_name'), F.lit(', '), F.col('last_name')).alias('user_full_name')
    ).show()

# COMMAND ----------

# Another way
users_df \
    .select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name'),
        F.concat(users_df['first_name'], F.lit(', '), users_df['last_name']).alias('user_full_name')
    ).show()

# COMMAND ----------

# Using withColumn and alias (first select and then withColumn)
users_df \
    .select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name')) \
        .withColumn('user_full_name', F.concat(F.col('user_first_name'), F.lit(', '), F.col('user_last_name'))) \
    .show()

# COMMAND ----------

# Using withColumn and alias (first withColumn and then select)
users_df \
    .withColumn('user_full_name', F.concat(F.col('first_name'), F.lit(', '), F.col('last_name'))) \
    .select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name'),
        'user_full_name'
    ) \
    .show()

# COMMAND ----------

required_columns = ['id', 'first_name', 'last_name', 'email', 'phone_numbers', 'courses']

target_column_names = ['user_id', 'user_first_name', 'user_last_name', 'user_email', 'user_phone_numbers', 'enrolled_courses']

# COMMAND ----------

help(users_df.toDF)

# COMMAND ----------

# Renaming all columns in one shot!
users_df \
    .select(required_columns) \
    .toDF(*target_column_names) \
    .show()

# COMMAND ----------


