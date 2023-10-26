# Databricks notebook source
import pyspark

# COMMAND ----------

df = sqlContext.sql("SELECT * FROM mydata_csv")

# COMMAND ----------

df.show()
