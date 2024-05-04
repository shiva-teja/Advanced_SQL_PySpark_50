# Databricks notebook source
# MAGIC %md
# MAGIC ## Customers who bought A and B but not C

# COMMAND ----------

customers_data = [[1, 'Daniel'], [2, 'Diana'], [3, 'Elizabeth'], [4, 'Jhon']]
customers_columns=['customer_id', 'customer_name']

customers_df = spark.createDataFrame(customers_data, customers_columns)
customers_df.show()

# COMMAND ----------

orders_data = [[10, 1, 'A'], [20, 1, 'B'], [30, 1, 'D'], [40, 1, 'C'], [50, 2, 'A'], [60, 3, 'A'], [70, 3, 'B'], [80, 3, 'D'], [90, 4, 'C']]
orders_column = ['order_id', 'customer_id', 'product_name']

orders_df = spark.createDataFrame(orders_data, orders_column)

orders_df.show()

# COMMAND ----------

from pyspark.sql.functions import col
final_orders = orders_df.groupBy("customer_id").pivot("product_name").count()
final_orders.show()

# COMMAND ----------

from pyspark.sql.functions import col, isnull
final_df = final_orders.alias('o').join(customers_df.alias('c'), col("o.customer_id") == col("c.customer_id"))

# final_df.show()

df = final_df.filter((col("A") == 1) & (col("B") == 1) & (isnull(col("C")))).select(col("customer_name")).distinct()
df.show()

# COMMAND ----------


