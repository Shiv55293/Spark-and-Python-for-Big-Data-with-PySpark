# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("lr_example").getOrCreate

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

# COMMAND ----------

data = sqlContext.sql("SELECT * FROM ecommerce_customers_csv")

# COMMAND ----------

data.show()

# COMMAND ----------

data.printSchema()

# COMMAND ----------

for item in data.head(1)[0]:print(item)

# COMMAND ----------

from pyspark.ml.linalg import Vectors

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

data.columns

# COMMAND ----------

assembler = VectorAssembler(inputCols=['Avg Session Length',
 'Time on App',
 'Time on Website',
 'Length of Membership'],outputCol='features')

# COMMAND ----------

output = assembler.transform(data)

# COMMAND ----------

output.printSchema()

# COMMAND ----------

output.head(1)

# COMMAND ----------

final_data = output.select('features','Yearly Amount Spent')

# COMMAND ----------

final_data.show()

# COMMAND ----------

train_data, test_data = final_data.randomSplit([0.7,0.3])

# COMMAND ----------

train_data.describe().show()

# COMMAND ----------

test_data.describe().show()

# COMMAND ----------

lr = LinearRegression(labelCol='Yearly Amount Spent')

# COMMAND ----------

lr_model = lr.fit(train_data)

# COMMAND ----------

test_results = lr_model.evaluate(test_data)

# COMMAND ----------

test_results.residuals.show()

# COMMAND ----------

test_results.rootMeanSquaredError

# COMMAND ----------

test_results.r2

# COMMAND ----------

final_data.describe().show()
