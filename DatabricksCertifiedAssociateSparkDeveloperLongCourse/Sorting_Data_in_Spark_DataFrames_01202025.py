# Databricks notebook source
# Sorting Data in Spark DataFrames
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

help(users_df.sort)

# COMMAND ----------

help(users_df.orderBy)

# COMMAND ----------

users_df.sort('first_name').show()

# COMMAND ----------

users_df.sort(users_df.first_name).show()

# COMMAND ----------

users_df.sort(users_df['first_name']).show()

# COMMAND ----------

users_df.sort(F.col('first_name')).show()

# COMMAND ----------

users_df.orderBy(F.col('first_name')).show()

# COMMAND ----------

users_df.orderBy(F.col('customer_from').asc()).show()

# COMMAND ----------

users_df.sort(F.size('courses').asc()).show()

# COMMAND ----------

users_df.sort('first_name', ascending=False).show()

# COMMAND ----------

users_df.sort(F.col('first_name').desc()).show()

# COMMAND ----------

users_df.sort(F.desc(F.col('first_name'))).show()

# COMMAND ----------

users_df.sort(F.col('customer_from').desc()).show()

# COMMAND ----------

users_df.orderBy(F.col('customer_from').desc_nulls_last()).show()

# COMMAND ----------

users_df.orderBy(F.col('customer_from').asc_nulls_first()).show()

# COMMAND ----------

courses = [
    {
        'course_id': 1,
        'course_name': 'Python for Data Science',
        'suitable_for': 'Advanced',
        'enrollment': 179498,
        'stars': 4.8,
        'number_of_ratings': 49972
    },
    {
        'course_id': 2,
        'course_name': 'Data Science for Business',
        'suitable_for': 'Intermediate',
        'enrollment': 123456,
        'stars': 4.6,
        'number_of_ratings': 34567
    },
    {
        'course_id': 3,
        'course_name': 'Machine Learning for Finance',
        'suitable_for': 'Beginner',
        'enrollment': 87654,
        'stars': 4.7,
        'number_of_ratings': 23456
    },
    {
        'course_id': 4,
        'course_name': 'Data Science for Social Good',
        'suitable_for': 'Advanced',
        'enrollment': 98765,
        'stars': 4.9,
        'number_of_ratings': 12345
    }
]

# COMMAND ----------

from pyspark.sql import Row

courses_df = spark.createDataFrame([Row(**course) for course in courses])

# COMMAND ----------

courses_df.dtypes

# COMMAND ----------

courses_df.show()

# COMMAND ----------

courses_df \
    .orderBy('suitable_for', 'enrollment') \
    .show()

# COMMAND ----------

courses_df \
    .orderBy(F.col('suitable_for').asc(), F.col('enrollment').desc()) \
    .show()

# COMMAND ----------

courses_df.sort(['suitable_for', 'number_of_ratings'], ascending=[1, 0]).show()

# COMMAND ----------

course_level = F.when(F.col('suitable_for') == 'Beginner', 0).otherwise(F.when(F.col('suitable_for') == 'Intermediate', 1).otherwise(2))

# COMMAND ----------

courses_df \
    .orderBy(course_level, F.col('number_of_ratings').desc()) \
    .show()

# COMMAND ----------

course_level = F.expr('CASE WHEN suitable_for = "Beginner" THEN 0 WHEN suitable_for = "Intermediate" THEN 1 ELSE 2 END')

# COMMAND ----------

courses_df \
    .orderBy(course_level, F.col('number_of_ratings').desc()) \
    .show()

# COMMAND ----------


