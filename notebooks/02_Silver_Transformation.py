from pyspark.sql import functions as F

print("===== SILVER LAYER TRANSFORMATION STARTED =====")


# =========================================================
# 1. READ BRONZE TABLES
# =========================================================

orders_df = spark.table("workspace.freshmart_bronze.orders")
order_items_df = spark.table("workspace.freshmart_bronze.order_items")
customers_df = spark.table("workspace.freshmart_bronze.customers")
delivery_df = spark.table("workspace.freshmart_bronze.delivery_logs")


# =========================================================
# 2. REMOVE DUPLICATE RECORDS
# =========================================================

orders_df = orders_df.dropDuplicates()
order_items_df = order_items_df.dropDuplicates()
customers_df = customers_df.dropDuplicates()
delivery_df = delivery_df.dropDuplicates()


# =========================================================
# 3. TYPE CASTING
# =========================================================

# Orders
if "order_date" in orders_df.columns:
    orders_df = orders_df.withColumn(
        "order_date",
        F.to_date("order_date")
    )

if "total_amount" in orders_df.columns:
    orders_df = orders_df.withColumn(
        "total_amount",
        F.col("total_amount").cast("double")
    )

# Order Items
if "quantity" in order_items_df.columns:
    order_items_df = order_items_df.withColumn(
        "quantity",
        F.col("quantity").cast("int")
    )

if "unit_price" in order_items_df.columns:
    order_items_df = order_items_df.withColumn(
        "unit_price",
        F.col("unit_price").cast("double")
    )

if "price" in order_items_df.columns:
    order_items_df = order_items_df.withColumn(
        "price",
        F.col("price").cast("double")
    )


# =========================================================
# 4. NULL HANDLING
# =========================================================

# Remove records where important IDs are missing

if "order_id" in orders_df.columns:
    orders_df = orders_df.filter(
        F.col("order_id").isNotNull()
    )

if "customer_id" in orders_df.columns:
    orders_df = orders_df.filter(
        F.col("customer_id").isNotNull()
    )

if "order_id" in order_items_df.columns:
    order_items_df = order_items_df.filter(
        F.col("order_id").isNotNull()
    )

if "customer_id" in customers_df.columns:
    customers_df = customers_df.filter(
        F.col("customer_id").isNotNull()
    )


# =========================================================
# 5. CLEAN STRING COLUMNS
# =========================================================

def clean_strings(df):
    for col_name, data_type in df.dtypes:
        if data_type == "string":
            df = df.withColumn(
                col_name,
                F.trim(F.col(col_name))
            )
    return df


orders_df = clean_strings(orders_df)
order_items_df = clean_strings(order_items_df)
customers_df = clean_strings(customers_df)
delivery_df = clean_strings(delivery_df)


# =========================================================
# 6. PII MASKING
# =========================================================

# Mask email using SHA-256

if "email" in customers_df.columns:
    customers_df = customers_df.withColumn(
        "email",
        F.sha2(F.col("email"), 256)
    )

# Mask phone using SHA-256

if "phone" in customers_df.columns:
    customers_df = customers_df.withColumn(
        "phone",
        F.sha2(F.col("phone"), 256)
    )

if "mobile" in customers_df.columns:
    customers_df = customers_df.withColumn(
        "mobile",
        F.sha2(F.col("mobile"), 256)
    )


# =========================================================
# 7. HANDLE REMAINING NULLS
# =========================================================

orders_df = orders_df.fillna({
    "status": "Unknown"
})

order_items_df = order_items_df.fillna({
    "quantity": 0
})

delivery_df = delivery_df.fillna({
    "delivery_status": "Unknown"
})


# =========================================================
# 8. SAVE SILVER DELTA TABLES
# =========================================================

orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.freshmart_silver.orders")


order_items_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.freshmart_silver.order_items")


customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.freshmart_silver.customers")


delivery_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.freshmart_silver.delivery_logs")


# =========================================================
# 9. VALIDATION
# =========================================================

print("===== SILVER LAYER CREATED SUCCESSFULLY =====")

print("Orders:", orders_df.count())
print("Order Items:", order_items_df.count())
print("Customers:", customers_df.count())
print("Delivery Logs:", delivery_df.count())

print("===== SILVER TRANSFORMATION COMPLETE =====")
