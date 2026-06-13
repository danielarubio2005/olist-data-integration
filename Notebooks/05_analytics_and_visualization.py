# Databricks notebook source
#Load the integrated dataset and register it as a SQL view

ANALYTICS_PATH = "/Volumes/workspace/default/olist_analytics"

fact = spark.read.parquet(f"{ANALYTICS_PATH}/fact_orders")
fact.createOrReplaceTempView("fact_orders")  #".createOrReplaceTempView" to write actual SQL against the data frame, as it registers the DataFrame as a SQL table (view) named fact_orders that can be queried with SQL. Tables are not stored amywhere but as temporary views in memory. Only volumes are being used to store data (raw, cleaned, analytics).

print(f"Loaded fact_orders: {fact.count()} rows")

# COMMAND ----------

#Install Plotly for nicer charts (the free Edition has matplotlib by default but I think Plotly looks better)

%pip install plotly
dbutils.library.restartPython()  #to use updated packages

# COMMAND ----------

ANALYTICS_PATH = "/Volumes/workspace/default/olist_analytics"
fact = spark.read.parquet(f"{ANALYTICS_PATH}/fact_orders")
fact.createOrReplaceTempView("fact_orders")
print("View recreated.") #".restartPython" of the cell 2 resets the session, so the view goes away, so to re-create it I run cell 1 again

# COMMAND ----------

# MAGIC %md
# MAGIC 1. How are sales evolving over time? 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW monthly_sales AS
# MAGIC SELECT
# MAGIC     purchase_year,
# MAGIC     purchase_month,
# MAGIC     COUNT(DISTINCT order_id) AS num_orders,
# MAGIC     ROUND(SUM(total_payment_value), 2) AS total_revenue
# MAGIC FROM fact_orders
# MAGIC WHERE purchase_year IS NOT NULL
# MAGIC GROUP BY purchase_year, purchase_month
# MAGIC ORDER BY purchase_year, purchase_month;
# MAGIC
# MAGIC SELECT * FROM monthly_sales;

# COMMAND ----------

#visualize the result as a line chart

import plotly.express as px

df = spark.sql("SELECT * FROM monthly_sales").toPandas()
df["period"] = df["purchase_year"].astype(str) + "-" + df["purchase_month"].astype(str).str.zfill(2)

fig = px.line(
    df, x="period", y="total_revenue",
    title="Monthly Revenue Trend (Olist, 2016–2018)",
    markers=True,
    labels={"period": "Month", "total_revenue": "Revenue (BRL)"}
)
fig.update_layout(width=900, height=450)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Which product categories generate the most revenue? 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW top_categories AS
# MAGIC SELECT
# MAGIC     product_category_name_english AS category,
# MAGIC     COUNT(DISTINCT order_id) AS num_orders,
# MAGIC     ROUND(SUM(total_price), 2) AS total_revenue,
# MAGIC     ROUND(AVG(review_score), 2) AS avg_rating
# MAGIC FROM fact_orders
# MAGIC WHERE product_category_name_english IS NOT NULL
# MAGIC   AND product_category_name_english != 'unknown'
# MAGIC GROUP BY product_category_name_english
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC SELECT * FROM top_categories;

# COMMAND ----------

#visualize the result as a bar chart

import plotly.express as px

df = spark.sql("SELECT * FROM top_categories").toPandas()

fig = px.bar(
    df.sort_values("total_revenue"),
    x="total_revenue", y="category",
    orientation="h",
    title="Top 10 Product Categories by Revenue",
    labels={"total_revenue": "Revenue (BRL)", "category": "Category"},
    color="avg_rating",
    color_continuous_scale="RdYlGn",
    range_color=[3.5, 5]
)
fig.update_layout(width=900, height=500)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Which Brazilian states are the biggest markets? 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW state_sales AS
# MAGIC SELECT
# MAGIC     customer_state AS state,
# MAGIC     COUNT(DISTINCT order_id) AS num_orders,
# MAGIC     ROUND(SUM(total_payment_value), 2) AS total_revenue,
# MAGIC     ROUND(AVG(total_payment_value), 2) AS avg_order_value
# MAGIC FROM fact_orders
# MAGIC WHERE customer_state IS NOT NULL
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC SELECT * FROM state_sales;

# COMMAND ----------

#visualize the result as a bar chart

import plotly.express as px

df = spark.sql("SELECT * FROM state_sales").toPandas()

fig = px.bar(
    df.head(15),
    x="state", y="total_revenue",
    title="Top 15 Brazilian States by Revenue",
    labels={"state": "State", "total_revenue": "Revenue (BRL)"},
    color="num_orders",
    color_continuous_scale="Blues"
)
fig.update_layout(width=900, height=450)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 4. How does delivery time relate to customer satisfaction?

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW delivery_satisfaction AS
# MAGIC SELECT
# MAGIC     review_score,
# MAGIC     COUNT(*) AS num_orders,
# MAGIC     ROUND(AVG(delivery_days), 1) AS avg_delivery_days,
# MAGIC     ROUND(AVG(delivery_delay_days), 1) AS avg_delay_days
# MAGIC FROM fact_orders
# MAGIC WHERE review_score IS NOT NULL
# MAGIC   AND delivery_days IS NOT NULL
# MAGIC   AND delivery_days BETWEEN 0 AND 60
# MAGIC GROUP BY review_score
# MAGIC ORDER BY review_score;
# MAGIC
# MAGIC SELECT * FROM delivery_satisfaction;

# COMMAND ----------

#visualize the result as a bar chart

import plotly.express as px

df = spark.sql("SELECT * FROM delivery_satisfaction").toPandas()

fig = px.bar(
    df, x="review_score", y="avg_delivery_days",
    title="Average Delivery Time vs Customer Review Score",
    labels={"review_score": "Review Score (1–5)", "avg_delivery_days": "Avg Delivery Days"},
    color="avg_delivery_days",
    color_continuous_scale="RdYlGn_r",
    text="avg_delivery_days"
)
fig.update_traces(textposition="outside")
fig.update_layout(width=800, height=450)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 5. What payment methods do customers prefer? 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW payment_dist AS
# MAGIC SELECT
# MAGIC     primary_payment_type AS payment_type,
# MAGIC     COUNT(*) AS num_orders,
# MAGIC     ROUND(AVG(total_payment_value), 2) AS avg_order_value
# MAGIC FROM fact_orders
# MAGIC WHERE primary_payment_type IS NOT NULL
# MAGIC GROUP BY primary_payment_type
# MAGIC ORDER BY num_orders DESC;
# MAGIC
# MAGIC SELECT * FROM payment_dist;

# COMMAND ----------

#visualize the result as a pie chart

import plotly.express as px

df = spark.sql("SELECT * FROM payment_dist").toPandas()

fig = px.pie(
    df, names="payment_type", values="num_orders",
    title="Payment Method Distribution",
    hole=0.4
)
fig.update_layout(width=700, height=500)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Are sellers delivering on time? 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW delivery_performance AS
# MAGIC SELECT
# MAGIC     purchase_year,
# MAGIC     purchase_month,
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     SUM(CASE WHEN delivery_delay_days <= 0 THEN 1 ELSE 0 END) AS on_time_orders,
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(CASE WHEN delivery_delay_days <= 0 THEN 1 ELSE 0 END) / COUNT(*),
# MAGIC         2
# MAGIC     ) AS on_time_pct
# MAGIC FROM fact_orders
# MAGIC WHERE delivery_delay_days IS NOT NULL
# MAGIC   AND purchase_year IS NOT NULL
# MAGIC GROUP BY purchase_year, purchase_month
# MAGIC ORDER BY purchase_year, purchase_month;
# MAGIC
# MAGIC SELECT * FROM delivery_performance;

# COMMAND ----------

#visualize the result as a line chart

import plotly.express as px

df = spark.sql("SELECT * FROM delivery_performance").toPandas()
df["period"] = df["purchase_year"].astype(str) + "-" + df["purchase_month"].astype(str).str.zfill(2)

fig = px.line(
    df, x="period", y="on_time_pct",
    title="On-Time Delivery Rate Over Time",
    markers=True,
    labels={"period": "Month", "on_time_pct": "On-Time Delivery %"}
)
fig.add_hline(y=90, line_dash="dash", line_color="red",
              annotation_text="90% target", annotation_position="right")
fig.update_layout(width=900, height=450, yaxis=dict(range=[70, 100]))
fig.show()