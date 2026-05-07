from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# create Spark session
spark = (
    SparkSession.builder
    .appName("Ecommerce ETL Pipeline")
    .master("local[*]")
    .getOrCreate()
)

# define schemas for raw data
orders_raw_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("discount_pct", StringType(), True),
])

order_items_raw_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("category", StringType(), True),
])

customers_raw_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("country", StringType(), True),
    StructField("customer_tier", StringType(), True),
    StructField("email", StringType(), True),
])

returns_raw_schema = StructType([
    StructField("return_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("return_date", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("refund_amount", StringType(), True),
])

# load raw data with defined schemas
orders_df = spark.read.option("header", True).schema(orders_raw_schema).csv("data/data/orders.csv")

customers_df = spark.read.option("header", True).schema(customers_raw_schema).csv("data/data/customers.csv")

order_items_df = spark.read.option("header", True).schema(order_items_raw_schema).csv("data/data/order_items.csv")

returns_df = spark.read.option("header", True).schema(returns_raw_schema).csv("data/data/returns.csv")

# helper function for mixed date formats
def parse_mixed_date(column_name):
    return F.coalesce(
        F.to_date(F.col(column_name), "yyyy-MM-dd"),
        F.to_date(F.col(column_name), "dd/MM/yyyy")
    )
# schema casting and rejection of invalid records

# orders casting
orders_casted_df = (
    orders_df
    .withColumn("order_date_casted", parse_mixed_date("order_date"))
    .withColumn("total_amount_casted", F.col("total_amount").cast(DoubleType()))
    .withColumn("discount_pct_casted", F.col("discount_pct").cast(DoubleType()))
)

rejected_orders_df = orders_casted_df.filter(
    (
        F.col("order_date").isNotNull() &
        F.col("order_date_casted").isNull()
    )
    |
    (
        F.col("total_amount").isNotNull() &
        F.col("total_amount_casted").isNull()
    )
    |
    (
        F.col("discount_pct").isNotNull() &
        F.col("discount_pct_casted").isNull()
    )
)

orders_clean_casted_df = (
    orders_casted_df
    .filter(
        ~(
            (
                F.col("order_date").isNotNull() &
                F.col("order_date_casted").isNull()
            )
            |
            (
                F.col("total_amount").isNotNull() &
                F.col("total_amount_casted").isNull()
            )
            |
            (
                F.col("discount_pct").isNotNull() &
                F.col("discount_pct_casted").isNull()
            )
        )
    )
    .select(
        "order_id",
        "customer_id",
        F.col("order_date_casted").alias("order_date"),
        "status",
        F.col("total_amount_casted").alias("total_amount"),
        F.col("discount_pct_casted").alias("discount_pct")
    )
)

# customers casting
customers_casted_df = (
    customers_df
    .withColumn("signup_date_casted", parse_mixed_date("signup_date"))
)

rejected_customers_df = customers_casted_df.filter(
    F.col("signup_date").isNotNull() &
    F.col("signup_date_casted").isNull()
)

customers_clean_casted_df = (
    customers_casted_df
    .filter(
        ~(
            F.col("signup_date").isNotNull() &
            F.col("signup_date_casted").isNull()
        )
    )
    .select(
        "customer_id",
        F.col("signup_date_casted").alias("signup_date"),
        "country",
        "customer_tier",
        "email"
    )
)

# order items casting

order_items_casted_df = (
    order_items_df
    .withColumn("quantity_casted", F.col("quantity").cast(IntegerType()))
    .withColumn("unit_price_casted", F.col("unit_price").cast(DoubleType()))
)

rejected_order_items_df = order_items_casted_df.filter(
    (
        F.col("quantity").isNotNull() &
        F.col("quantity_casted").isNull()
    )
    |
    (
        F.col("unit_price").isNotNull() &
        F.col("unit_price_casted").isNull()
    )
)

order_items_clean_casted_df = (
    order_items_casted_df
    .filter(
        ~(
            (
                F.col("quantity").isNotNull() &
                F.col("quantity_casted").isNull()
            )
            |
            (
                F.col("unit_price").isNotNull() &
                F.col("unit_price_casted").isNull()
            )
        )
    )
    .select(
        "item_id",
        "order_id",
        "product_id",
        F.col("quantity_casted").alias("quantity"),
        F.col("unit_price_casted").alias("unit_price"),
        "category"
    )
)

# returns casting

returns_casted_df = (
    returns_df
    .withColumn("return_date_casted", parse_mixed_date("return_date"))
    .withColumn("refund_amount_casted", F.col("refund_amount").cast(DoubleType()))
)

rejected_returns_df = returns_casted_df.filter(
    (
        F.col("return_date").isNotNull() &
        F.col("return_date_casted").isNull()
    )
    |
    (
        F.col("refund_amount").isNotNull() &
        F.col("refund_amount_casted").isNull()
    )
)

returns_clean_casted_df = (
    returns_casted_df
    .filter(
        ~(
            (
                F.col("return_date").isNotNull() &
                F.col("return_date_casted").isNull()
            )
            |
            (
                F.col("refund_amount").isNotNull() &
                F.col("refund_amount_casted").isNull()
            )
        )
    )
    .select(
        "return_id",
        "order_id",
        F.col("return_date_casted").alias("return_date"),
        "reason",
        F.col("refund_amount_casted").alias("refund_amount")
    )
)


# 2. data quality and cleaning

# 1. Remove exact duplicates
# 2. Standardize customer_tier to lowercase
# 3. Drop rows where order_id or customer_id is NULL
# 4. Flag negative order amounts

orders_clean_df = (
    orders_clean_casted_df
    .dropDuplicates()
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .withColumn(
        "is_negative_amount",
        F.when(F.col("total_amount") < 0, True).otherwise(False)
    )
)

customers_clean_df = (
    customers_clean_casted_df
    .dropDuplicates()
    .filter(F.col("customer_id").isNotNull())
    .withColumn("customer_tier", F.lower(F.col("customer_tier")))
)

order_items_clean_df = (
    order_items_clean_casted_df
    .dropDuplicates()
    .filter(F.col("item_id").isNotNull())
    .filter(F.col("order_id").isNotNull())
)

returns_clean_df = (
    returns_clean_casted_df
    .dropDuplicates()
    .filter(F.col("return_id").isNotNull())
    .filter(F.col("order_id").isNotNull())
)


print("Clean orders:", orders_clean_df.count())
print("Rejected orders:", rejected_orders_df.count())

print("Clean customers:", customers_clean_df.count())
print("Rejected customers:", rejected_customers_df.count())

print("Clean order items:", order_items_clean_df.count())
print("Rejected order items:", rejected_order_items_df.count())

print("Clean returns:", returns_clean_df.count())
print("Rejected returns:", rejected_returns_df.count())

spark.stop()