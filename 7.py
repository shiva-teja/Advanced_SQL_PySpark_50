# Databricks notebook source
Customer_tbl = [[101, 'Alice'], [102, 'Bob'], [103, 'Charlie']]
customer = ['customer_id', 'customer_name']
Orders_tbl =[[1, '2020-03-01', 1500, 101, 1], [2, '2020-05-25', 2400, 102, 2], [3, '2019-05-25', 800, 101, 3], [4, '2020-09-13', 1000, 103, 2], [5, '2019-02-11',700, 101, 2]]
orders = ['order_id', 'sale_date', 'order_cost', 'customer_id', 'seller_id']
Seller_tbl =[[1, 'Daniel'], [2, 'Elizabeth'], [3, 'Frank']]
seller = ['seller_id', 'seller_name']

customer_df=spark.createDataFrame(Customer_tbl, customer)
orders_df=spark.createDataFrame(Orders_tbl, orders)
seller_df=spark.createDataFrame(Seller_tbl, seller)



customer_df.show()
orders_df.show()
seller_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, year, isnull

orders_final_df = orders_df.filter(year('sale_date') == 2020)

orders_final_df.show()

final_df = seller_df.alias('s').join(orders_final_df.alias('o'), col("s.seller_id") == col("o.seller_id"), "LEFT")

no_order_sellers = final_df.filter(col("o.order_id").isNull())

last_df = no_order_sellers.select('s.seller_name').orderBy('s.seller_name')


last_df.show()
# 

# COMMAND ----------



# Filter Orders_tbl for orders from the year 2020
orders_2020 = orders_df.filter(year("sale_date") == 2020)

# Perform a LEFT JOIN between Seller_tbl and the filtered Orders_tbl
result_df = seller_df.alias("s").join(
    orders_2020.alias("o"),
    col("s.seller_id") == col("o.seller_id"),
    "left_outer"
)

# Filter to get sellers with no orders in 2020
sellers_without_orders = result_df.filter(col("o.order_id").isNull())

# Select seller_name and order by seller_name
final_df = sellers_without_orders.select("s.seller_name").orderBy("s.seller_name")

# Show the results
final_df.show()


# COMMAND ----------


