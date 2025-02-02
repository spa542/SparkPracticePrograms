# Databricks notebook source
# Dropping Columns from Spark DataFrames
import datetime
import pyspark.sql.functions as F
from pyspark.sql import Row
import pandas as pd

users = [
    {
        'id': 1,
        'first_name': 'Corrie',
        'last_name': 'Brown',
        'email': 'corrie@etsy.com',
        'gender': 'male',
        'current_city': 'Dallas',
        'phone_numbers': Row(mobile='+1 234 567 890', home='+1 123 456 789'),
        'courses': [1, 2],
        'is_customer': True,
        'amount_paid': 1000.55,
        'customer_from': datetime.date(2018, 1, 1),
        'last_updated_ts': datetime.datetime(2019, 1, 1, 12, 0, 0)
    },
    {
        'id': 2,
        'first_name': 'Jane',
        'last_name': 'Doe',
        'email': 'jane@etsy.com',
        'gender': 'male',
        'current_city': None,
        'phone_numbers': Row(mobile='+1 234 567 890', home=None),
        'courses': [1, 2],
        'is_customer': True,
        'amount_paid': None,
        'customer_from': None,
        'last_updated_ts': datetime.datetime(2019, 1, 1, 12, 0, 0)
    }
]

# COMMAND ----------

users_df = spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

users_df.show()

# COMMAND ----------

help(users_df.drop)

# COMMAND ----------

users_df.drop('last_updated_ts').printSchema()

# COMMAND ----------

users_df.drop(users_df['last_updated_ts']).printSchema()

# COMMAND ----------

users_df.drop(F.col('user_id')).printSchema()

# COMMAND ----------

users_df.drop('first_name', 'last_name').printSchema()

# COMMAND ----------

users_df.drop(*['first_name', 'last_name']).printSchema()

# COMMAND ----------

# Dropping Duplicates
users_df.drop_duplicates().show()

# COMMAND ----------

users_df.drop_duplicates(['first_name', 'last_name']).show()

# COMMAND ----------

help(users_df.drop_duplicates)

# COMMAND ----------

help(users_df.dropDuplicates)

# COMMAND ----------

# Dropping Null Values
users_df.count()

# COMMAND ----------

users_df.na

# COMMAND ----------

help(users_df.dropna)

# COMMAND ----------

help(users_df.na.drop)

# COMMAND ----------

users_df.na.drop('any').show()

# COMMAND ----------

users_df.na.drop('all').show()

# COMMAND ----------

users_df.na.drop(thresh=2).show()

# COMMAND ----------

users_df.na.drop(how='all', subset=['amount_paid', 'customer_from']).show()

# COMMAND ----------


