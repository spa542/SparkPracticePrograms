# Databricks notebook source
ages_list = [21, 23, 18, 41, 32]

# COMMAND ----------

type(ages_list)

# COMMAND ----------

spark

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

spark.createDataFrame(ages_list, 'int')

# COMMAND ----------

from pyspark.sql.types import *

spark.createDataFrame(ages_list, IntegerType())

# COMMAND ----------

names_list = ['Scott', 'Donald', 'Mickey']

# COMMAND ----------

spark.createDataFrame(names_list, StringType())

# COMMAND ----------

ages_list = [(21,), (32,), (43,)]

# COMMAND ----------

type(ages_list)

# COMMAND ----------

type(ages_list[2])

# COMMAND ----------

# Not needed to specify the schema since it can infer based on multiple columns
spark.createDataFrame(ages_list)

# COMMAND ----------

# Schema can be specified as a SQL expression
spark.createDataFrame(ages_list, schema="age INT")

# COMMAND ----------

users_list = [(1, 'Scott'), (2, 'Donald'), (3, 'Mickey'), (4, 'Elvis')]

spark.createDataFrame(users_list, 'user_id INT, user_first_name STRING')

# COMMAND ----------

# Overview of Spark Row
users_list = [(1, 'Scott'), (2, 'Donald'), (3, 'Mickey'), (4, 'Elvis')]
df = spark.createDataFrame(users_list, 'user_id INT, user_first_name STRING')

df.show()

# COMMAND ----------

df.collect()

# COMMAND ----------

type(df.collect())

# COMMAND ----------

from pyspark.sql import Row

help(Row)

# COMMAND ----------

r = Row('Alice', 11)

r

# COMMAND ----------

row2 = Row(name='Alice', age=11)

row2

# COMMAND ----------

row2.name

# COMMAND ----------

row2['name']

# COMMAND ----------

# Convert a List of Lists into a Spark DataFrame using Row
users_list = [[1, 'Alice'], [2, 'Bob'], [3, 'Charlie']]

type(users_list)

# COMMAND ----------

type(users_list[1])

# COMMAND ----------

spark.createDataFrame(users_list)

# COMMAND ----------

spark.createDataFrame(users_list, 'user_id INT, user_first_name STRING')

# COMMAND ----------

users_rows = [Row(uid, uname) for (uid, uname) in users_list]
# Another way using *args
#users_rows = [Row(*user) for user in users_list]

users_rows

# COMMAND ----------

spark.createDataFrame(users_rows, 'user_id INT, user_first_name STRING')

# COMMAND ----------

# Convert List of Tuples into Spark Dataframe using Row
users_list = [(1, 'Scott'), (2, 'Donald'), (3, 'Mickey'), (4, 'Elvis')]

type(users_list[1])

# COMMAND ----------

spark.createDataFrame(users_list, 'user_id INT, user_first_name STRING')

# COMMAND ----------

users_rows = [Row(uid, uname) for uid, uname in users_list]

spark.createDataFrame(users_rows, 'user_id INT, user_first_name STRING')

# COMMAND ----------

# Convert a List of Dicts into Spark DataFrame using Row
# NOTE: This is being deprecated!!!! Must use pyspark.sql.Row instead!!!!
users_list = [
    {'user_id': 1, 'user_first_name': 'Alice'},
    {'user_id': 2, 'user_first_name': 'Bob'},
    {'user_id': 3, 'user_first_name': 'Charlie'}
]

spark.createDataFrame(users_list)

# COMMAND ----------

# The correct way...
users_rows = [Row(**item) for item in users_list]

spark.createDataFrame(users_rows, 'user_id INT, user_first_name STRING')

# COMMAND ----------

# Basic Spark Data Types
import datetime
from pyspark.sql import Row

users = [
    {
        'id': 1,
        'first_name': 'Corrie',
        'last_name': 'Ferguson',
        'email': 'krome4@shutterfly.com',
        'is_customer': False,
        'amount_paid': 250.55,
        'customer_from': datetime.date(2021, 1, 15),
        'last_updated_at': datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

# COMMAND ----------

users_df = spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.printSchema()

# COMMAND ----------

users_df.columns

# COMMAND ----------

users_df.dtypes

# COMMAND ----------

# Some special types are array, map, and struct
# Specify a schema for the DataFrame using string

users = [
    {
        'id': 1,
        'first_name': 'Corrie',
        'last_name': 'Ferguson',
        'email': 'krome4@shutterfly.com',
        'is_customer': False,
        'amount_paid': 250.55,
        'customer_from': datetime.date(2021, 1, 15),
        'last_updated_at': datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

users_schema = '''
    id LONG,
    first_name STRING,
    last_name STRING,
    email STRING,
    is_customer BOOLEAN,
    amount_paid DOUBLE,
    customer_from DATE,
    last_updated_at TIMESTAMP
'''

spark.createDataFrame(users, schema=users_schema).show()

# COMMAND ----------

# Create a DataFrame using List with Strings

from pyspark.sql.types import *

help(spark.createDataFrame)

# COMMAND ----------

users_schema = ['id', 'first_name', 'last_name', 'email', 'is_customer', 'amount_paid', 'customer_from', 'last_updated_at']

spark.createDataFrame(users, schema=users_schema)

# COMMAND ----------

# Create DataFrame with Schema using Spark objects
from pyspark.sql.types import *

fields = StructType([
    StructField('id', LongType(), True),
    StructField('first_name', StringType(), False),
    StructField('last_name', StringType(), True),
    StructField('email', StringType(), True),
    StructField('is_customer', BooleanType(), True),
    StructField('amount_paid', DoubleType(), True),
    StructField('customer_from', DateType(), True),
    StructField('last_updated_at', TimestampType(), True)
])

spark.createDataFrame(users, schema=fields).show()

# COMMAND ----------

# Create Spark DataFrame using Pandas DataFrame
import pandas as pd

users = [
    {
        'id': 5,
        'first_name': 'Kurt',
        'last_name': 'Rome',
        'email': 'krome4@shutterfly.com',
        'is_customer': False,
        'last_updated_ts': datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

# COMMAND ----------

pd.DataFrame(users)

# COMMAND ----------

sample_df = pd.DataFrame(users)

spark.createDataFrame(sample_df).printSchema()

# COMMAND ----------

# Overview of Spark Special Data Types
# Array, Struct, and Map

# COMMAND ----------

# Array type Columns

from pyspark.sql import Row

users = [
    {
        'id': 5,
        'first_name': 'Kurt',
        'last_name': 'Rome',
        'email': 'krome4@shutterfly.com',
        'phone_numbers': ['1 871 463 7142', '1 555 463 7142'],
        'is_customer': False,
        'last_updated_ts': datetime.datetime(2021, 4, 2, 0, 55, 18)
    },
    {
        'id': 5,
        'first_name': 'Kurt',
        'last_name': 'Rome',
        'email': 'krome4@shutterfly.com',
        'phone_numbers': [],
        'is_customer': False,
        'last_updated_ts': datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

users_df = spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

users_df.printSchema()

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.select('id', 'phone_numbers').show(truncate=False)

# COMMAND ----------

users_df.columns

# COMMAND ----------

users_df.dtypes

# COMMAND ----------

import pyspark.sql.functions as F

users_df.withColumn('phone_number', F.explode('phone_numbers')).drop('phone_numbers').show()

users_df.withColumn('phone_number', F.explode_outer('phone_numbers')).drop('phone_numbers').show() # Any records with 0 columns will be included in this output

# COMMAND ----------

users_df.select('id', F.col('phone_numbers')[0].alias('mobile'), F.col('phone_numbers')[1].alias('home')).show()

# COMMAND ----------

# Map Type Columns in Spark DataFrames
users = [
    {
        'id': 5,
        'first_name': 'Kurt',
        'last_name': 'Rome',
        'email': 'krome4@shutterfly.com',
        'phone_numbers': {'mobile': '1 871 463 7142', 'home': '1 555 463 7142'},
        'is_customer': False,
        'last_updated_ts': datetime.datetime(2021, 4, 2, 0, 55, 18)
    },
    {
        'id': 5,
        'first_name': 'Kurt',
        'last_name': 'Rome',
        'email': 'krome4@shutterfly.com',
        'phone_numbers': {},
        'is_customer': False,
        'last_updated_ts': datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

users_df = spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

users_df.printSchema()

# COMMAND ----------

users_df.show(truncate=False)

# COMMAND ----------

users_df.select(F.col('phone_numbers')['mobile']).show() # Can rename if we want

# COMMAND ----------

users_df.select('id', F.explode('phone_numbers')).show()

users_df.select('id', F.explode_outer('phone_numbers')).show()

# COMMAND ----------

users_df.select('id', F.explode('phone_numbers')) \
    .withColumnRenamed('key', 'phone_type') \
    .withColumnRenamed('value', 'phone_number') \
    .drop('phone_numbers').show()

# COMMAND ----------

# Struct Type Columns in Spark DataFrames

users = [
    {
        'id': 5,
        'first_name': 'Kurt',
        'last_name': 'Rome',
        'email': 'krome4@shutterfly.com',
        'phone_numbers': Row(mobile='1 871 463 7142', home='1 555 463 7142'),
        'is_customer': False,
        'last_updated_ts': datetime.datetime(2021, 4, 2, 0, 55, 18)
    },
    {
        'id': 5,
        'first_name': 'Kurt',
        'last_name': 'Rome',
        'email': 'krome4@shutterfly.com',
        'phone_numbers': Row(mobile=None, home=None),
        'is_customer': False,
        'last_updated_ts': datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

# COMMAND ----------

users_df = spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

users_df.show()

# COMMAND ----------

users_df.select('id', 'phone_numbers').show(truncate=False)

# COMMAND ----------

users_df.columns

# COMMAND ----------

users_df.dtypes

# COMMAND ----------

users_df.select('id', 'phone_numbers.mobile', 'phone_numbers.home').show()

# COMMAND ----------

users_df.select('id', 'phone_numbers.*').show()

# COMMAND ----------


