# Databricks notebook source
# MAGIC %md
# MAGIC ## Customer who never ordered

# COMMAND ----------

customers_data = [[1, 'Joe'], [2, 'Henry'], [3, 'Sam'], [4, 'Max']]
customers_columns = ['id', 'name']

customers_df = spark.createDataFrame(customers_data, customers_columns)

customers_df.show()

orders_data = [[1, 3], [2, 1]]
orders_columns = ['id', 'customerId']

orders_df = spark.createDataFrame(orders_data, orders_columns)

orders_df.show()

# COMMAND ----------

from pyspark.sql.functions import col

final_df = customers_df.alias("c").join(orders_df.alias("o"), col("c.id") == col("o.customerId"), "LEFT").filter(col("o.id").isNull()).select(col("c.name").alias("Name"))



# COMMAND ----------

final_df.show()

# COMMAND ----------


