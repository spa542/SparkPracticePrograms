# Databricks notebook source
# Filtering Data from Spark DataFrames
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

spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', False)

# COMMAND ----------

users_df = spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

users_df.show()

# COMMAND ----------

help(users_df.filter)

# COMMAND ----------

help(users_df.where)

# COMMAND ----------

users_df.filter(F.col('id') == 1).show()

# COMMAND ----------

users_df.where(F.col('id') == 1).show()

# COMMAND ----------

users_df.filter(users_df['id'] == 1).show()

# COMMAND ----------

users_df.filter('id = 1').show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')
spark.sql('SELECT * FROM users WHERE id = 1').show()

# COMMAND ----------

users_df.filter(F.col('is_customer') == True).show()

# COMMAND ----------

users_df.filter(F.col('is_customer') == 'true').show()

# COMMAND ----------

users_df.filter('is_customer = true').show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM users WHERE is_customer = true

# COMMAND ----------

users_df.filter(F.col('current_city') == 'Dallas').show()

# COMMAND ----------

users_df.filter('amount_paid == 900.0').show()

# COMMAND ----------

users_df.filter(F.isnan('amount_paid') == True).show()

# COMMAND ----------

users_df.filter('isnan(amount_paid) = True').show()

# COMMAND ----------

users_df.select('id', 'current_city').filter(F.col('current_city') != 'Dallas').show()

# COMMAND ----------

users_df.select('id', 'current_city').filter((F.col('current_city') != 'Dallas') | (F.col('current_city').isNull())).show()

# COMMAND ----------

users_df.select('id', 'current_city').filter('current_city != "Dallas" OR current_city IS NULL').show()

# COMMAND ----------

users_df.select('id', 'current_city').filter(F.col('current_city') != '').show()

# COMMAND ----------

users_df.select('id', 'email', 'last_updated_ts').filter(F.col('last_updated_ts').between('2021-01-01', '2021-01-31')).show() # Can pass the timestamp here as well
# Timestamps for each day will need to be carrie do the next day or 23:59:59

# COMMAND ----------

users_df.select('id', 'email', 'last_updated_ts').filter('last_updated_ts BETWEEN "2021-01-01" AND "2021-01-31"').show()

# COMMAND ----------

users_df.select('id', 'amount_paid').filter('amount_paid BETWEEN 0 AND 1000').show()

# COMMAND ----------

users_df.select('id', 'current_city').filter(F.col('current_city').isNotNull()).show()

# COMMAND ----------

users_df.select('id', 'current_city').filter('current_city IS NOT NULL').show()

# COMMAND ----------

users_df.filter((F.col('current_city') == '') | (F.col('current_city').isNull())).show()

# COMMAND ----------

users_df.filter((F.col('current_city') == 'Houston') | (F.col('current_city') == 'Dallas')).show()

# COMMAND ----------

users_df.filter('current_city IN ("Houston", "Dallas")').show()

# COMMAND ----------

users_df.filter(F.col('current_city').isin(['Houston', 'Dallas'])).show()

# COMMAND ----------

users_df.select('id', 'amount_paid').filter((F.col('amount_paid') > 100) & (F.col('amount_paid').isNaN() == False)).show()

# COMMAND ----------

users_df.select('id', 'amount_paid').filter('amount_paid > 100 AND isnan(amount_paid) = False').show()

# COMMAND ----------

users_df.select('id', 'amount_paid').filter((F.col('amount_paid') < 10000) & (F.col('amount_paid').isNaN() == False)).show()

# COMMAND ----------

users_df.select('id', 'customer_from').filter('customer_from > "2017-01-01"').show()

# COMMAND ----------

users_df.select('id', 'gender', 'is_customer').filter((F.col('gender') == 'male') & (F.col('is_customer') == True)).show()

# COMMAND ----------

users_df.select('id', 'gender', 'is_customer').filter('gender = "male" AND is_customer = true').show()

# COMMAND ----------

users_df.select('id', 'email', 'current_city', 'is_customer').filter((F.col('current_city') == '') | (F.col('is_customer') == False)).show()

# COMMAND ----------

users_df.select('id', 'email', 'current_city', 'is_customer').filter('current_city = "" OR is_customer = false').show()

# COMMAND ----------


