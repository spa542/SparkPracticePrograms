# Databricks notebook source
# Manipulating Columns in Spark DataFrames

orders = spark.read.csv(
    '/public/retail_db/orders',
    schema='order_id INT, order_date STRING, order_customer_id INT, order_status STRING'
)

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

orders.show()

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

help(F.date_format)

# COMMAND ----------

orders.select('*', F.date_format('order_date', 'yyyyMM').alias('order_month')).show()

# COMMAND ----------

orders.withColumn('order_month', F.date_format('order_date', 'yyyyMM')).show()

# COMMAND ----------

orders.filter(F.date_format('order_date', 'yyyyMM') == '201401').show()

# COMMAND ----------

orders.groupBy(F.date_format('order_date', 'yyyyMM').alias('order_month')).count().show()

# COMMAND ----------

# Create a Dummy DataFrame
l = [('X',)]

# COMMAND ----------

df = spark.createDataFrame(l, 'dummy STRING')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

df.select(F.current_date()).show()

# COMMAND ----------

df.select(F.current_date().alias('current_date')).show()

# COMMAND ----------

employees = [
    (1, "Scott", "Tiger", 1000.0, "united states", "+1 123 456 7890", "456 78 9123"),
    (2, "Henry", "Ford", 1250.0, "india", "+91 234 567 8901", "456 78 9123")
]

# COMMAND ----------

len(employees)

# COMMAND ----------

employeesDF = spark.createDataFrame(
    employees,
    schema="""
        employee_id INT, first_name STRING, last_name STRING, salary FLOAT, nationality STRING, phone_number STRING, ssn STRING
    """
)

# COMMAND ----------

employeesDF.printSchema()

# COMMAND ----------

help(F.lower)

# COMMAND ----------

employeesDF.withColumn('nationality', F.upper(F.col('nationality'))).show()

# COMMAND ----------

help(F.date_format)

# COMMAND ----------

help(F.col)

# COMMAND ----------

help(F.lit)

# COMMAND ----------

help(help)

# COMMAND ----------

help(F.concat)

# COMMAND ----------

help(F.concat_ws)

# COMMAND ----------

employeesDF.select('first_name', 'last_name').show()

# COMMAND ----------

employeesDF.groupBy('nationality').count().show()

# COMMAND ----------

employeesDF.orderBy('employee_id').show()

# COMMAND ----------

help(F.col)

# COMMAND ----------

type(F.col('first_name'))

# COMMAND ----------

employeesDF.select(F.col('first_name'), F.col('last_name')).show()

# COMMAND ----------

help(F.upper)

# COMMAND ----------

employeesDF.select(F.upper(F.col('first_name')), F.upper(F.col('last_name'))).show()

# COMMAND ----------

from pyspark.sql.column import Column

help(Column)

# COMMAND ----------

employeesDF.orderBy(F.col('employee_id').desc()).show()

# COMMAND ----------

employeesDF.orderBy(F.upper(employeesDF['first_name']).alias('first_name')).show() # Just sorting by upper case, not actually projecting here

# COMMAND ----------

employeesDF.orderBy(F.upper(employeesDF.first_name).alias('first_name')).show()

# COMMAND ----------

employeesDF.select(F.concat(F.col('first_name'), F.lit(', '), F.col('last_name')).alias('full_name')).show(truncate=False)

# COMMAND ----------

help(F.lit)

# COMMAND ----------

employeesDF.withColumn('bonus', F.col('salary') * F.lit(0.2)).show()

# COMMAND ----------

help(F.concat)

# COMMAND ----------

employeesDF.withColumn('full_name', F.concat(F.col('first_name'), F.col('last_name'))).show()

# COMMAND ----------

help(F.concat_ws)

# COMMAND ----------

employeesDF.withColumn('full_name', F.concat_ws(', ', F.col('first_name'), F.col('last_name'))).show()

# COMMAND ----------

employeesDF \
    .select('employee_id', 'nationality') \
    .withColumn('nationality_upper', F.upper(F.col('nationality'))) \
    .withColumn('nationality_lower', F.lower(F.col('nationality'))) \
    .withColumn('nationality_initcap', F.initcap(F.col('nationality'))) \
    .withColumn('nationality_length', F.length(F.col('nationality'))) \
    .show()

# COMMAND ----------

help(F.substring)

# COMMAND ----------

df.select(F.substring(F.lit('Hello World!'), 7, 5)).show()

# COMMAND ----------

df.select(F.substring(F.lit('Hello World!'), -5, 5)).show()

# COMMAND ----------

employeesDF.show(truncate=False)

# COMMAND ----------

employeesDF \
    .select('employee_id', 'phone_number', 'ssn') \
    .withColumn('phone_last4', F.substring(F.col('phone_number'), -4, 4).cast('int')) \
    .withColumn('ssn_last4', F.substring(F.col('ssn'), 8, 4).cast('int')) \
    .show()

# COMMAND ----------

employeesDF \
    .select('employee_id', 'phone_number', 'ssn') \
    .withColumn('phone_last4', F.substring('phone_number', -4, 4).cast('int')) \
    .withColumn('ssn_last4', F.substring('ssn', 8, 4).cast('int')) \
    .show()

# COMMAND ----------

help(F.split)

# COMMAND ----------

help(F.explode)

# COMMAND ----------

df.select(F.split(F.lit('Hello World, how are you'), ' ')) \
    .show(truncate=False)

# COMMAND ----------

df.select(F.split(F.lit('Hello World, how are you'), ' ')[2]) \
    .show(truncate=False)

# COMMAND ----------

df.select(F.explode(F.split(F.lit('Hello World, how are you'), ' ')).alias('word')).show(truncate=False)

# COMMAND ----------

employeesDF.select('employee_id', 'phone_number', 'ssn').withColumn('phone_number', F.explode(F.split('phone_number', ','))).show()

# COMMAND ----------

employeesDF \
    .select('employee_id', 'phone_number', 'ssn') \
    .withColumn('area_code', F.split('phone_number', ' ')[1].cast('int')) \
    .withColumn('phone_last4', F.split('phone_number', ' ')[3].cast('int')) \
    .withColumn('ssn_last4', F.split('ssn', ' ')[2].cast('int')) \
    .show()

# COMMAND ----------

employeesDF.groupBy('employee_id').count().show()

# COMMAND ----------

help(F.lpad)

# COMMAND ----------

df.select(F.lpad(F.lit('Hello'), 10, '-').alias('dummy')).show()

# COMMAND ----------

employeesDF.show()

# COMMAND ----------

employeesDF \
    .select(
        F.concat(F.lpad('employee_id', 5, '0'), F.rpad('first_name', 10, '-'), F.rpad('last_name', 10, '-'),
        F.lpad('salary', 10, '0'), F.rpad('nationality', 15, '-'), F.rpad('phone_number', 17, '-'), 'ssn').alias('employee')
    ).show(truncate=False)

# COMMAND ----------

help(F.ltrim)

# COMMAND ----------

help(F.rtrim)

# COMMAND ----------

help(F.trim)

# COMMAND ----------

df.withColumn('dummy', F.lit('     Hello      ')) \
    .withColumn('ltrim', F.ltrim('dummy')) \
    .withColumn('rtrim', F.rtrim('dummy')) \
    .withColumn('trim', F.trim('dummy')) \
    .show()

# COMMAND ----------

spark.sql('DESCRIBE FUNCTION trim').show(truncate=False)

# COMMAND ----------

df.withColumn('dummy', F.lit('     Hello      ')) \
    .withColumn('ltrim', F.expr("trim(LEADING ' ' FROM dummy)")) \
    .withColumn('rtrim', F.expr("trim(TRAILING '.' FROM rtrim(dummy))")) \
    .withColumn('trim', F.expr("trim(BOTH ' ' FROM dummy)")) \
    .show()

# COMMAND ----------

help(F.current_date)

# COMMAND ----------

df.select(F.current_date().alias('current_date')).show()

# COMMAND ----------

df.select(F.current_timestamp().alias('current_timestamp')).show(truncate=False)

# COMMAND ----------

help(F.to_date)

# COMMAND ----------

df.select(F.to_date(F.lit('20210228'), 'yyyyMMdd').alias('to_date')).show()

# COMMAND ----------

help(F.to_timestamp)

# COMMAND ----------

df.select(F.to_timestamp(F.lit('20210228 1725'), 'yyyyMMdd HHmm').alias('to_timestamp')).show()

# COMMAND ----------

help(F.date_add)

# COMMAND ----------

help(F.date_sub)

# COMMAND ----------

datetimes = [
    ('2014-02-28', '2014-02-28 10:00:00.123'),
    ('2016-02-29', '2016-02-29 08:08:08.999')
]

datetimesDF = spark.createDataFrame(datetimes, schema='date STRING, time STRING')

# COMMAND ----------

datetimesDF \
    .withColumn('date_add_date', F.date_add('date', 10)) \
    .withColumn('date_add_time', F.date_add('time', 10)) \
    .withColumn('date_sub_date', F.date_sub('date', 10)) \
    .withColumn('date_sub_time', F.date_sub('time', 10)) \
    .show(truncate=False)

# COMMAND ----------

help(F.datediff)

# COMMAND ----------

datetimesDF \
    .withColumn('datediff_date', F.datediff(F.current_date(), 'date')) \
    .withColumn('datediff_time', F.datediff(F.current_timestamp(), 'time')) \
    .show(truncate=False)

# COMMAND ----------

help(F.months_between)

# COMMAND ----------

help(F.add_months)

# COMMAND ----------

datetimesDF \
    .withColumn('months_between_date', F.round(F.months_between(F.current_date(), 'date'), 2)) \
    .withColumn('months_between_time', F.round(F.months_between(F.current_timestamp(), 'time'), 2)) \
    .withColumn('add_months_date', F.add_months('date', 3)) \
    .withColumn('add_months_time', F.add_months('time', 3)) \
    .show(truncate=False)

# COMMAND ----------

help(F.trunc)

# COMMAND ----------

datetimesDF \
    .withColumn('date_trunc', F.trunc('date', 'MM')) \
    .withColumn('time_trunc', F.trunc('time', 'yy')) \
    .show(truncate=False)

# COMMAND ----------

help(F.date_trunc)

# COMMAND ----------

datetimesDF \
    .withColumn('date_trunc', F.date_trunc('MM', 'date')) \
    .withColumn('time_trunc', F.date_trunc('yy', 'time')) \
    .withColumn('date_dt', F.date_trunc('HOUR', 'date')) \
    .withColumn('time_dt', F.date_trunc('HOUR', 'time')) \
    .withColumn('time_dt1', F.date_trunc('dd', 'time')) \
    .show(truncate=False)

# COMMAND ----------

help(F.year)

# COMMAND ----------

df.select(
    F.current_date().alias('current_date'),
    F.year(F.current_date()).alias('year'),
    F.month(F.current_date()).alias('month'),
    F.weekofyear(F.current_date()).alias('weekofyear'),
    F.dayofyear(F.current_date()).alias('dayofyear'),
    F.dayofmonth(F.current_date()).alias('dayofmonth'),
    F.dayofweek(F.current_date()).alias('dayofweek'),
    F.hour(F.current_date()).alias('hour'),
    F.minute(F.current_date()).alias('minute'),
    F.second(F.current_date()).alias('second')
).show()

# COMMAND ----------

df.select(F.to_date(F.lit('20210302'), 'yyyyMMdd').alias('to_date')).show()

# COMMAND ----------

df.select(F.to_date(F.lit('2021061'), 'yyyyDDD').alias('to_date')).show()

# COMMAND ----------

df.select(F.to_date(F.lit('02/03/2021'), 'dd/MM/yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(F.to_date(F.lit('02-Mar-2021'), 'dd-MMM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(F.to_date(F.lit('March 2, 2021'), 'MMMM d, yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(F.to_timestamp(F.lit('02-Mar-2021 17:30:15'), 'dd-MMM-yyyy HH:mm:ss').alias('to_timestamp')).show()

# COMMAND ----------

datetimesDF.printSchema()

# COMMAND ----------

datetimesDF \
    .withColumn('to_date', F.to_date(F.col('date').cast('string'), 'yyyy-MM-dd')) \
    .withColumn('to_timestamp', F.to_timestamp(F.col('time'), 'yyyy-MM-dd HH:mm:ss.SSS')) \
    .show(truncate=False)

# COMMAND ----------

help(F.date_format)

# COMMAND ----------

datetimesDF \
    .withColumn('date_ym', F.date_format('date', 'yyyyMM')) \
    .withColumn('time_ym', F.date_format('time', 'yyyyMM')) \
    .show(truncate=False)

# COMMAND ----------

# yyyy
# MM
# dd
# DD
# HH
# hh
# mm
# ss
# SSS

# COMMAND ----------

datetimesDF \
    .withColumn('date_ym', F.date_format('date', 'yyyyMM').cast('int')) \
    .withColumn('time_ym', F.date_format('time', 'yyyyMM').cast('int')) \
    .show(truncate=False)

# COMMAND ----------

datetimesDF \
    .withColumn('date_ym', F.date_format('date', 'yyyyMMddHHmmss').cast('long')) \
    .withColumn('time_ym', F.date_format('time', 'yyyyMMddHHmmss').cast('long')) \
    .show(truncate=False)

# COMMAND ----------

datetimesDF \
    .withColumn('date_yd', F.date_format('date', 'yyyyDDD').cast('int')) \
    .withColumn('time_yd', F.date_format('time', 'yyyyDDD').cast('int')) \
    .show(truncate=False)

# COMMAND ----------

datetimesDF.withColumn('date_desc', F.date_format('date', 'MMMM d, yyyy')).show(truncate=False)

# COMMAND ----------

datetimesDF.withColumn('day_name_full', F.date_format('date', 'EEEE')).show(truncate=False)

# COMMAND ----------

datetimes = [
    (20140228, '2014-02-28', '2014-02-28 10:00:00'),
    (20160229, '2016-02-29', '2016-02-29 08:08:00')
]

datetimesDF = spark.createDataFrame(datetimes).toDF('dateid', 'date', 'time')

# COMMAND ----------

help(F.unix_timestamp)

# COMMAND ----------

datetimesDF \
    .withColumn('unix_date_id', F.unix_timestamp(F.col('dateid').cast('string'), 'yyyyMMdd')) \
    .withColumn('unix_date', F.unix_timestamp('date', 'yyyy-MM-dd')) \
    .withColumn('unix_time', F.unix_timestamp('time')) \
    .show()

# COMMAND ----------

unixtimes = [
    (1393561800,),
    (1456713488,)
]

unixtimesDF = spark.createDataFrame(unixtimes).toDF('unixtime')

# COMMAND ----------

unixtimesDF.show()

# COMMAND ----------

help(F.from_unixtime)

# COMMAND ----------

unixtimesDF \
    .withColumn('date', F.from_unixtime('unixtime', 'yyyyMMdd')) \
    .withColumn('time', F.from_unixtime('unixtime')) \
    .show()

# COMMAND ----------

help(F.col('unixtime').cast)

# COMMAND ----------

unixtimesDF.select(F.col('unixtime').cast('timestamp')).show()

# COMMAND ----------

# Dealing with Null Values
help(F.coalesce)

# COMMAND ----------

employeesDF = employeesDF.withColumn('bonus', F.lit(1000))

employeesDF.withColumn('bonus', F.coalesce('bonus', F.lit(0))).show()

# COMMAND ----------

employeesDF.withColumn('bonus', F.col('bonus').cast('int')).show()

# COMMAND ----------

employeesDF.withColumn('bonus', F.coalesce(F.col('bonus').cast('int'), F.lit(0))).show()

# COMMAND ----------

employeesDF.withColumn('bonus', F.expr('nvl(bonus, 0)')).show()

# COMMAND ----------

employeesDF.withColumn('bonus', F.expr("nvl(nullif(bonus, ''), 0)")).show()

# COMMAND ----------

employeesDF.withColumn('payment', F.col('salary') + (F.col('salary') * F.coalesce(F.col('bonus').cast('int'), F.lit(0)) / 100)).show()

# COMMAND ----------

employeesDF.na

# COMMAND ----------

help(employeesDF.na)

# COMMAND ----------

help(employeesDF.na.fill)

# COMMAND ----------

help(employeesDF.na.drop)

# COMMAND ----------

employeesDF.fillna(0.0).show() # Will only fill the nulls where the data types match

# COMMAND ----------

employeesDF.fillna('na').show() # Will only update string types here

# COMMAND ----------

employeesDF.fillna(0.0).fillna('na').show()

# COMMAND ----------

employeesDF.fillna(0.0, 'salary').fillna('na', 'last_name').show()

# COMMAND ----------

employeesDF.withColumn('bonus', F.coalesce(F.col('bonus').cast('int'), F.lit(0))).show()

# COMMAND ----------

employeesDF.withColumn('bonus', F.expr("CASE WHEN bonus IS NULL or bonus = '' THEN 0 ELSE bonus END")).show()

# COMMAND ----------

F.when?

# COMMAND ----------

employeesDF.withColumn('bonus', F.when((F.col('bonus').isNull()) | (F.col('bonus') == F.lit('')), 0).otherwise(F.col('bonus'))).show()

# COMMAND ----------


