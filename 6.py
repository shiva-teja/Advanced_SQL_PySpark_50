# Databricks notebook source
pdata = [[1, 'Wang', 'Allen'], [2, 'Alice', 'Bob']]
pcolumns=['personId', 'firstName', 'lastName']

adata = [[1, 2, 'New York City', 'New York'], [2, 3, 'Leetcode', 'California']]
acolumns=['addressId', 'personId', 'city', 'state']

pdf = spark.createDataFrame(pdata, pcolumns)
adf = spark.createDataFrame(adata, acolumns)

# COMMAND ----------

from pyspark.sql.functions import col

final_df = pdf.alias('p').join(adf.alias('a'), col("p.personId") == col("a.personId"), "LEFT").select('p.firstName', 'p.lastName', 'a.city', 'a.state')

final_df.show()

# COMMAND ----------


