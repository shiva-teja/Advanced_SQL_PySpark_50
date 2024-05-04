# Databricks notebook source
# MAGIC %md
# MAGIC ## Highest Grade for each Student

# COMMAND ----------

data = [[2, 2, 95], [2, 3, 95], [1, 1, 90], [1, 2, 99], [3, 1, 80], [3, 2, 75], [3, 3, 82]]
columns=['student_id', 'course_id', 'grade']

stud_df = spark.createDataFrame(data, columns)

stud_df.show()

# COMMAND ----------

from pyspark.sql.functions import window, col, rank
from pyspark.sql import Window

window_spec = Window.partitionBy(col("student_id")).orderBy(col("grade").desc(), col("course_id"))

rank_df = stud_df.select('student_id', 'grade', 'course_id', rank().over(window_spec).alias('rank'))

final_df = rank_df.filter(col("rank")==1).select('student_id', 'grade', 'course_id')
final_df.show()


# COMMAND ----------


