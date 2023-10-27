# Databricks notebook source
data = sqlContext.sql("SELECT * FROM titanic_1_csv")

# COMMAND ----------

data.printSchema()

# COMMAND ----------

data.columns

# COMMAND ----------

my_cols = data.select(['Survived',
 'Pclass','Sex',
 'Age',
 'SibSp',
 'Parch', 'Fare','Embarked'])
 

# COMMAND ----------

final_data = my_cols.na.drop()

# COMMAND ----------

from pyspark.ml.feature import (VectorAssembler, VectorIndexer, OneHotEncoder, StringIndexer)

# COMMAND ----------

gender_indexer = StringIndexer(inputCol='Sex', outputCol='SexIndex')
gender_encoder = OneHotEncoder(inputCol='SexIndex',outputCol='SexVec')

# COMMAND ----------

embark_indexer = StringIndexer(inputCol='Embarked',outputCol='EmbarkIndex')
embark_encoder = OneHotEncoder(inputCol='EmbarkIndex',outputCol='EmbarkVec')

# COMMAND ----------

assembler = VectorAssembler(inputCols=['Pclass','SexVec','EmbarkVec','Age','SibSp','Parch','Fare',],outputCol='features')

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

# COMMAND ----------

from pyspark.ml import Pipeline

# COMMAND ----------

log_reg_titanic = LogisticRegression(featuresCol='features',labelCol='Survived')

# COMMAND ----------

pipeline = Pipeline(stages=[gender_indexer,embark_indexer,gender_encoder,embark_encoder,assembler,log_reg_titanic])

# COMMAND ----------

train_data, test_data = final_data.randomSplit([0.7,0.3])

# COMMAND ----------

fit_model = pipeline.fit(train_data)

# COMMAND ----------

results = fit_model.transform(test_data)

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator

# COMMAND ----------

my_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction',labelCol='Survived')

# COMMAND ----------

results.select('Survived','prediction').show()

# COMMAND ----------

AUC = my_eval.evaluate(results)

# COMMAND ----------

AUC
