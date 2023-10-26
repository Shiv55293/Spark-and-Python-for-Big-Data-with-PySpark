# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

df = sqlContext.sql("SELECT * FROM mydata_csv")

# COMMAND ----------

type(df['sales'])

# COMMAND ----------

df.select('sales').show()

# COMMAND ----------

df.head(2)[0]

# COMMAND ----------

type(df.head(2)[0])

# COMMAND ----------

df.select(['name','sales']).show()

# COMMAND ----------

df.withColumn('fullName',df['name']).show()

# COMMAND ----------

df.withColumn('Doule_sales',df['sales']*2).show()

# COMMAND ----------

df.show()

# COMMAND ----------

df.withColumnRenamed('name','fullName').show()

# COMMAND ----------

df.createOrReplaceTempView('mydata')

# COMMAND ----------

df.show()
