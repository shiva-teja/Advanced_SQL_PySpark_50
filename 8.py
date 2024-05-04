# Databricks notebook source
# MAGIC %md
# MAGIC ## Top Travellers

# COMMAND ----------

users_data = [[1, 'Alice'], [2, 'Bob'], [3, 'Alex'], [4, 'Donald'], [7, 'Lee'], [13, 'Jonathan'], [19, 'Elvis']]
users_columns=['id', 'name']
rides_data = [[1, 1, 120], [2, 2, 317], [3, 3, 222], [4, 7, 100], [5, 13, 312], [6, 19, 50], [7, 7, 120], [8, 19, 400], [9, 7, 230]]
rides_columns=['id', 'user_id', 'distance']

users_df=spark.createDataFrame(users_data, users_columns)
rides_df=spark.createDataFrame(rides_data, rides_columns)
users_df.show()
rides_df.show()

# COMMAND ----------

from pyspark.sql.functions import col,sum, coalesce, lit

combined_df = users_df.alias('u').join(rides_df.alias('r'), col("u.id") == col("r.user_id"), "LEFT")

final_df = combined_df.groupBy('u.id', 'u.name').agg(coalesce(sum('r.distance'), lit(0)).alias('travelled_distance'))

result = final_df.orderBy(col('travelled_distance').desc(), col('u.name'))

result.show()

# COMMAND ----------

from pyspark.sql.functions import when, sum, col, lit
from pyspark.sql.window import Window

windowSpec = Window.partitionBy("u.id").orderBy("u.id")

result_df = combined_df.withColumn(
    'travelled_distance', 
    when(col('r.distance').isNotNull(), sum("r.distance").over(windowSpec)).otherwise(lit(0)))

distinct_df = result_df.select("u.name", "travelled_distance").distinct()


final_df = distinct_df.orderBy(col("travelled_distance").desc(), col("u.name"))

final_df.show()


# COMMAND ----------


