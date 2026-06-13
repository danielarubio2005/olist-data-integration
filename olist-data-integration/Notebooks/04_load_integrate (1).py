# Databricks notebook source
#Load the cleaned Parquet

CLEAN_PATH = "/Volumes/workspace/default/olist_clean"

orders        = spark.read.parquet(f"{CLEAN_PATH}/orders")
order_items   = spark.read.parquet(f"{CLEAN_PATH}/order_items")
payments      = spark.read.parquet(f"{CLEAN_PATH}/payments")
reviews       = spark.read.parquet(f"{CLEAN_PATH}/reviews")
products      = spark.read.parquet(f"{CLEAN_PATH}/products")
customers     = spark.read.parquet(f"{CLEAN_PATH}/customers")
sellers       = spark.read.parquet(f"{CLEAN_PATH}/sellers")

print("All cleaned tables loaded from Parquet")

# COMMAND ----------

#Aggregate payments per order (an order can have multiple payment rows):

from pyspark.sql.functions import sum as _sum, countDistinct, first #as _sum alias so it doesn't clash with Python's sum()

payments_per_order = (
    payments
    .groupBy("order_id")
    .agg(
        _sum("payment_value").alias("total_payment_value"),  #sum all the payments of the order into total_payment_value
        countDistinct("payment_type").alias("num_payment_types"),  #count the different payment types
        first("payment_type").alias("primary_payment_type"),  #record one of the payment types as the primary one
    )
)

payments_per_order.show(5)

# COMMAND ----------

#Aggregate order items per order (an order can have multiple items):

from pyspark.sql.functions import count

items_per_order = (
    order_items
    .groupBy("order_id")
    .agg(
        count("order_item_id").alias("num_items"),
        _sum("price").alias("total_price"),
        _sum("freight_value").alias("total_freight"),
        first("seller_id").alias("primary_seller_id"), #for orders with multiple items, I picked one seller/product as   
        first("product_id").alias("primary_product_id"), #representative to keep the fact table at one row per order
    )
)

items_per_order.show(5)

# COMMAND ----------

#Integrate all of the tables into a single fact_orders table by left-joining orders to customers, aggregated items, aggregated payments, reviews, products, and sellers:

# Rename product_id and seller_id to match the column names in items_per_order, so all joins below can use the same on="column_name" syntax

products_renamed = (
    products
    .select("product_id", "product_category_name_english")
    .withColumnRenamed("product_id", "primary_product_id")
)

sellers_renamed = sellers.withColumnRenamed("seller_id", "primary_seller_id")

fact_orders = (
    orders
    .join(customers, on="customer_id", how="left")
    .join(items_per_order, on="order_id", how="left")
    .join(payments_per_order, on="order_id", how="left")
    .join(reviews.select("order_id", "review_score"), on="order_id", how="left")
    .join(products_renamed, on="primary_product_id", how="left")
    .join(sellers_renamed, on="primary_seller_id", how="left")
)

print(f"Final integrated table: {fact_orders.count():,} rows, {len(fact_orders.columns)} columns")
fact_orders.printSchema()

#From 9 individual CSV files to one table ready for analysis.

# COMMAND ----------

#Add useful derived columns that can enrich the analysis (specially for delivery time analysis, like its relation to review score):

from pyspark.sql.functions import datediff, year, month, dayofweek

fact_orders = (
    fact_orders
    .withColumn("delivery_days",
                datediff("order_delivered_customer_date", "order_purchase_timestamp")) #days between purchase and delivery
    .withColumn("estimated_delivery_days",
                datediff("order_estimated_delivery_date", "order_purchase_timestamp"))
    .withColumn("delivery_delay_days",
                datediff("order_delivered_customer_date", "order_estimated_delivery_date")) #delivery delay days
    .withColumn("purchase_year",      year("order_purchase_timestamp")) #show year, month and day of week of purchase
    .withColumn("purchase_month",     month("order_purchase_timestamp"))
    .withColumn("purchase_dayofweek", dayofweek("order_purchase_timestamp"))
)

fact_orders.select(
    "order_id", "delivery_days", "delivery_delay_days",
    "purchase_year", "purchase_month", "review_score", "total_payment_value"
).show(5)

# COMMAND ----------

#Save the final integrated table:

ANALYTICS_PATH = "/Volumes/workspace/default/olist_analytics" #once again the volume is created in the default schema for consistency, to have all 3 under the same schema.

(fact_orders
    .write
    .mode("overwrite")
    .partitionBy("purchase_year", "purchase_month") #partitionBy to splits the Parquet files by year/month so future queries that filter by date are faster
    .parquet(f"{ANALYTICS_PATH}/fact_orders"))

print("Final integrated dataset written to Parquet, partitioned by year/month")

# COMMAND ----------

# Read back the saved data to confirm total row count, column count and data range covered (Verification)
from pyspark.sql.functions import min as _min, max as _max

fact = spark.read.parquet(f"{ANALYTICS_PATH}/fact_orders")
print(f"Final row count: {fact.count():,}")
print(f"Final column count: {len(fact.columns)}")

date_range = fact.agg(
    _min("order_purchase_timestamp").alias("first_order"),
    _max("order_purchase_timestamp").alias("last_order")
).collect()[0]

print(f"Date range: {date_range['first_order']} to {date_range['last_order']}")