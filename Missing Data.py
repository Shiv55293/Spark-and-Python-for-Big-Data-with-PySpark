# Databricks notebook source
from pyspark.sql import SparkSession


# COMMAND ----------

spark = SparkSession.Builder.appName('miss').getOrCreate()

# COMMAND ----------

df = sqlContext.sql("SELECT * FROM containnull_1_csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.na.drop().show()

# COMMAND ----------

df.na.drop(thresh=2).show()

# COMMAND ----------

df.na.drop(how='any').show()

# COMMAND ----------

df.na.drop(how='all').show()

# COMMAND ----------

df.na.drop(subset=['Sales']).show()

# COMMAND ----------

df.na.fill('Fill Value').show()
