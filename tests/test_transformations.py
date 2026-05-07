import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .appName("Ecommerce ETL Unit Tests")
        .master("local[*]")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


def clean_orders_for_test(orders_df):
    return (
        orders_df
        .dropDuplicates()
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .withColumn(
            "is_negative_amount",
            F.when(F.col("total_amount") < 0, True).otherwise(False)
        )
    )


def clean_customers_for_test(customers_df):
    return (
        customers_df
        .dropDuplicates()
        .filter(F.col("customer_id").isNotNull())
        .withColumn("customer_tier", F.lower(F.col("customer_tier")))
    )


def calculate_net_amount_for_test(orders_df):
    return orders_df.withColumn(
        "net_amount",
        F.col("total_amount") * (1 - F.col("discount_pct") / 100)
    )


def test_clean_orders_removes_null_order_id_and_customer_id(spark):
    data = [
        ("O001", "C001", 1000.0, 10.0),
        (None, "C002", 500.0, 5.0),
        ("O003", None, 700.0, 0.0),
        ("O004", "C004", 900.0, 15.0),
    ]

    columns = ["order_id", "customer_id", "total_amount", "discount_pct"]

    orders_df = spark.createDataFrame(data, columns)

    cleaned_df = clean_orders_for_test(orders_df)

    assert cleaned_df.count() == 2

    remaining_order_ids = [
        row["order_id"]
        for row in cleaned_df.select("order_id").collect()
    ]

    assert "O001" in remaining_order_ids
    assert "O004" in remaining_order_ids


def test_clean_orders_flags_negative_amounts_without_dropping_them(spark):
    data = [
        ("O001", "C001", 1000.0, 10.0),
        ("O002", "C002", -500.0, 5.0),
    ]

    columns = ["order_id", "customer_id", "total_amount", "discount_pct"]

    orders_df = spark.createDataFrame(data, columns)

    cleaned_df = clean_orders_for_test(orders_df)

    assert cleaned_df.count() == 2

    negative_flag = (
        cleaned_df
        .filter(F.col("order_id") == "O002")
        .select("is_negative_amount")
        .collect()[0][0]
    )

    assert negative_flag is True


def test_clean_customers_standardizes_customer_tier_to_lowercase(spark):
    data = [
        ("C001", "Gold", "Kenya"),
        ("C002", "SILVER", "Uganda"),
        ("C003", "bronze", "Tanzania"),
    ]

    columns = ["customer_id", "customer_tier", "country"]

    customers_df = spark.createDataFrame(data, columns)

    cleaned_df = clean_customers_for_test(customers_df)

    tiers = [
        row["customer_tier"]
        for row in cleaned_df.select("customer_tier").collect()
    ]

    assert "gold" in tiers
    assert "silver" in tiers
    assert "bronze" in tiers


def test_net_amount_is_calculated_correctly(spark):
    data = [
        ("O001", 1000.0, 10.0),
        ("O002", 2000.0, 25.0),
    ]

    columns = ["order_id", "total_amount", "discount_pct"]

    orders_df = spark.createDataFrame(data, columns)

    result_df = calculate_net_amount_for_test(orders_df)

    order_1_net_amount = (
        result_df
        .filter(F.col("order_id") == "O001")
        .select("net_amount")
        .collect()[0][0]
    )

    order_2_net_amount = (
        result_df
        .filter(F.col("order_id") == "O002")
        .select("net_amount")
        .collect()[0][0]
    )

    assert order_1_net_amount == 900.0
    assert order_2_net_amount == 1500.0