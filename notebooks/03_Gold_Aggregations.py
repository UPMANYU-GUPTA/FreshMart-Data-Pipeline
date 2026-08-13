from pyspark.sql import functions as F

print("===== GOLD LAYER AGGREGATION STARTED =====")


# =========================================================
# 1. READ SILVER TABLES
# =========================================================

orders_df = spark.table("workspace.freshmart_silver.orders")
order_items_df = spark.table("workspace.freshmart_silver.order_items")
customers_df = spark.table("workspace.freshmart_silver.customers")
delivery_df = spark.table("workspace.freshmart_silver.delivery_logs")


# =========================================================
# 2. DAILY REVENUE BY CITY
# =========================================================

orders_city_df = orders_df

if "order_date" in orders_city_df.columns:
    orders_city_df = orders_city_df.withColumn(
        "order_date",
        F.to_date("order_date")
    )

daily_revenue_city = orders_city_df.groupBy(
    "order_date",
    "city"
).agg(
    F.sum("total_amount").alias("total_revenue"),
    F.countDistinct("order_id").alias("total_orders")
).orderBy(
    "order_date",
    "city"
)

daily_revenue_city.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.freshmart_gold.daily_revenue_by_city"
    )


# =========================================================
# 3. DELIVERY PERFORMANCE
# =========================================================

delivery_gold = delivery_df

# Calculate delivery duration if dates are available
if "order_date" in delivery_gold.columns:
    delivery_gold = delivery_gold.withColumn(
        "order_date",
        F.to_date("order_date")
    )

if "delivery_date" in delivery_gold.columns:
    delivery_gold = delivery_gold.withColumn(
        "delivery_date",
        F.to_date("delivery_date")
    )

if "order_date" in delivery_gold.columns and \
   "delivery_date" in delivery_gold.columns:

    delivery_gold = delivery_gold.withColumn(
        "delivery_days",
        F.datediff(
            F.col("delivery_date"),
            F.col("order_date")
        )
    )

    delivery_performance = delivery_gold.groupBy(
        "delivery_status"
    ).agg(
        F.count("*").alias("total_deliveries"),
        F.round(
            F.avg("delivery_days"), 2
        ).alias("average_delivery_days")
    )

else:

    delivery_performance = delivery_gold.groupBy(
        "delivery_status"
    ).agg(
        F.count("*").alias("total_deliveries")
    )


delivery_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.freshmart_gold.delivery_performance"
    )


# =========================================================
# 4. PRODUCT SALES / RETURN RATE
# =========================================================

product_sales = order_items_df

# Calculate item revenue
if "quantity" in product_sales.columns and \
   "unit_price" in product_sales.columns:

    product_sales = product_sales.withColumn(
        "item_revenue",
        F.col("quantity") * F.col("unit_price")
    )

elif "quantity" in product_sales.columns and \
     "price" in product_sales.columns:

    product_sales = product_sales.withColumn(
        "item_revenue",
        F.col("quantity") * F.col("price")
    )


# Product level aggregation
product_group_columns = []

if "product_id" in product_sales.columns:
    product_group_columns.append("product_id")

if "product_name" in product_sales.columns:
    product_group_columns.append("product_name")

if product_group_columns:

    product_sales_gold = product_sales.groupBy(
        *product_group_columns
    ).agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("item_revenue").alias("total_revenue")
    )

    product_sales_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(
            "workspace.freshmart_gold.product_sales"
        )


# =========================================================
# 5. CUSTOMER REVENUE
# =========================================================

customer_revenue = orders_df.groupBy(
    "customer_id"
).agg(
    F.sum("total_amount").alias("total_revenue"),
    F.countDistinct("order_id").alias("total_orders"),
    F.round(
        F.avg("total_amount"), 2
    ).alias("average_order_value")
)


# Add customer information where available
if "customer_id" in customers_df.columns:

    customer_columns = ["customer_id"]

    if "city" in customers_df.columns:
        customer_columns.append("city")

    if "customer_name" in customers_df.columns:
        customer_columns.append("customer_name")

    customer_info = customers_df.select(
        *customer_columns
    ).dropDuplicates(["customer_id"])

    customer_revenue = customer_revenue.join(
        customer_info,
        on="customer_id",
        how="left"
    )


customer_revenue.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.freshmart_gold.customer_revenue"
    )


# =========================================================
# 6. TOP CUSTOMERS
# =========================================================

top_customers = customer_revenue.orderBy(
    F.desc("total_revenue")
).limit(10)

top_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.freshmart_gold.top_customers"
    )


# =========================================================
# 7. VALIDATION
# =========================================================

print("===== GOLD LAYER CREATED SUCCESSFULLY =====")

print(
    "Daily Revenue by City:",
    spark.table(
        "workspace.freshmart_gold.daily_revenue_by_city"
    ).count()
)

print(
    "Delivery Performance:",
    spark.table(
        "workspace.freshmart_gold.delivery_performance"
    ).count()
)

print(
    "Customer Revenue:",
    spark.table(
        "workspace.freshmart_gold.customer_revenue"
    ).count()
)

print(
    "Top Customers:",
    spark.table(
        "workspace.freshmart_gold.top_customers"
    ).count()
)

print("===== GOLD AGGREGATION COMPLETE =====")
