# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('aggs').getOrCreate()

# COMMAND ----------

df = sqlContext.sql("SELECT * FROM sales_info_csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.groupBy("Company")

# COMMAND ----------

df.groupBy('Company').mean().show()

# COMMAND ----------

df.groupBy('Company').sum().show()

# COMMAND ----------

df.groupBy('Company').max().show()

# COMMAND ----------

df.groupBy('Company').min().show()

# COMMAND ----------

df.groupBy('Company').count().show()

# COMMAND ----------

df.agg({'Sales':'sum'}).show()

# COMMAND ----------

df.agg({'Sales':'max'}).show()

# COMMAND ----------

group_data = df.groupBy('Company')

# COMMAND ----------

group_data.agg({'Sales':'max'}).show()

# COMMAND ----------

from pyspark.sql.functions import countDistinct,avg,stddev

# COMMAND ----------

from pyspark.sql.functions import countDistinct

# COMMAND ----------

df.select(countDistinct('Sales')).show()

# COMMAND ----------

from pyspark.sql.functions import avg

# COMMAND ----------

df.select(avg('Sales')).show()

# COMMAND ----------

df.select(avg('Sales').alias('Average Sales')).show()

# COMMAND ----------

from pyspark.sql.functions import stddev

# COMMAND ----------

df.select(stddev('Sales')).show()

# COMMAND ----------

df.orderBy('Sales').show()

# COMMAND ----------

df.orderBy(df['Sales'].desc()).show()
