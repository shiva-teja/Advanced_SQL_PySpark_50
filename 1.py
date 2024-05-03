# Databricks notebook source
# MAGIC %md
# MAGIC ## Customers with Postive Revenue in Year 2021

# COMMAND ----------



from pyspark.sql.functions import col

data = [['1', '2018', '50'], ['1', '2021', '30'], ['1', '2020', '70'], ['2', '2021', '-50'], ['3', '2018', '10'], ['3', '2016', '50'], ['4', '2021', '20']]

columns=['customer_id', 'year', 'revenue']

df = spark.createDataFrame(data, columns)

df.show()

final_df = df.select(col("customer_id")).filter((col("year")==2021) & (col("revenue") > 0))

final_df.show()


# COMMAND ----------


