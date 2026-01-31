## Loading transactions.json

transactions_df = spark.read.option("multiline", "true").json(
    "file:///C:/Users/pc/OneDrive/Desktop/AUCA/third_trim/Big_data_analytics/FinalProject/transactions.json"
)
## verifying
transactions_df.printSchema()

##Basic Spark Exploration
transactions_df.count()
transactions_df.select("status").groupBy("status").count().show()
transactions_df.select("payment_method").groupBy("payment_method").count().show()

###Identifying Missing
from pyspark.sql.functions import col, count, when

transactions_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in transactions_df.columns
]).show(truncate=False)

## Normalize Timestamp Format
from pyspark.sql.functions import to_timestamp

clean_transactions_df = clean_transactions_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)
## Normalize Nested Items
from pyspark.sql.functions import explode

clean_items_df = clean_transactions_df.select(
    "transaction_id",
    "user_id",
    "timestamp",
    explode("items").alias("item")
)

##normalize item fields
clean_items_df = clean_items_df.select(
    "transaction_id",
    "user_id",
    "timestamp",
    col("item.product_id").alias("product_id"),
    col("item.quantity").alias("quantity"),
    col("item.unit_price").alias("unit_price"),
    col("item.subtotal").alias("subtotal")
)

## Batch Analysis

## seld-join on transactions
from pyspark.sql.functions import col

pairs_df = (
    normalized_items_df.alias("a")
    .join(
        normalized_items_df.alias("b"),
        on="transaction_id"
    )
    .where(col("a.product_id") < col("b.product_id"))
    .select(
        col("a.product_id").alias("product_1"),
        col("b.product_id").alias("product_2")
    )
)

## Counting c0-0ccurences
recommendation_df = (
    pairs_df
    .groupBy("product_1", "product_2")
    .count()
    .orderBy(col("count").desc())
)
# Complex Spark SQL Query (Aggregation)
## Which products generate the highest total revenue?
spark.sql("""
SELECT
    product_id,
    SUM(subtotal) AS total_revenue,
    SUM(quantity) AS total_quantity
FROM transaction_items
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 10
""").show(truncate=False)

## Visualization 
## sales over time
import matplotlib.pyplot as plt

sales_over_time_df = (
    normalized_items_df
    .withColumn("date", normalized_items_df.timestamp.cast("date"))
    .groupBy("date")
    .sum("subtotal")
    .orderBy("date")
)

sales_pd = sales_over_time_df.toPandas()

plt.figure()
plt.plot(sales_pd["date"], sales_pd["sum(subtotal)"])
plt.xlabel("Date")
plt.ylabel("Total Sales")
plt.title("Sales Performance Over Time")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

## Top products
top_products_df = (
    normalized_items_df
    .groupBy("product_id")
    .sum("quantity")
    .orderBy("sum(quantity)", ascending=False)
    .limit(10)
)

top_products_pd = top_products_df.toPandas()

plt.figure()
plt.bar(top_products_pd["product_id"], top_products_pd["sum(quantity)"])
plt.xlabel("Product ID")
plt.ylabel("Total Quantity Sold")
plt.title("Top 10 Selling Products")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# conversion funnel
import matplotlib.pyplot as plt

funnel_df = (
    sessions_df
    .groupBy("conversion_status")
    .count()
)

funnel_pd = funnel_df.toPandas()

plt.figure()
plt.bar(funnel_pd["conversion_status"], funnel_pd["count"])
plt.xlabel("Funnel Stage")
plt.ylabel("Number of Users")
plt.title("Conversion Funnel")
plt.tight_layout()
plt.show()

## Transaction distribution
status_df = (
    transactions_df
    .groupBy("status")
    .count()
)

status_rows = status_df.collect()
labels = [r["status"] for r in status_rows]
counts = [r["count"] for r in status_rows]

plt.figure(figsize=(6,4))
plt.bar(labels, counts)
plt.xlabel("Transaction Status")
plt.ylabel("Number of Transactions")
plt.title("Transaction Status Distribution")
plt.tight_layout()
plt.show()

## Product quantity sold
qqty_df = (
    items_df
    .groupBy("product_id")
    .agg(_sum("quantity").alias("total_quantity"))
    .orderBy(col("total_quantity").desc())
    .limit(10)
)

qty_rows = qty_df.collect()
products = [r["product_id"] for r in qty_rows]
quantities = [r["total_quantity"] for r in qty_rows]

plt.figure(figsize=(8,4))
plt.bar(products, quantities)
plt.xlabel("Product ID")
plt.ylabel("Quantity Sold")
plt.title("Top 10 Products by Quantity Sold")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
