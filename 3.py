# Databricks notebook source
# MAGIC %md
# MAGIC ## Calculate Special bonus

# COMMAND ----------

data = [[2, 'Meir', 3000], [3, 'Michael', 3800], [7, 'Addilyn', 7400], [8, 'Juan', 6100], [9, 'Kannon', 7700]]

columns = ['employee_id', 'name', 'salary']

df = spark.createDataFrame(data, columns)

df.show()

# COMMAND ----------

from pyspark.sql.functions import col,lower,when
final_df = df.select(col('employee_id'), when((col("employee_id")%2 == 1) & (~lower(col("name")).like('m%')), col("salary")).otherwise(0).alias("Bonus"))
final_df.show()      

# COMMAND ----------


