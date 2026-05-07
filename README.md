# E-Commerce PySpark ETL Pipeline
Candidate: Candidate 7

This repository contains a PySpark ETL pipeline for an e-commerce data engineering assessment.

The pipeline ingests raw CSV files, applies schema enforcement, handles data quality issues, performs joins and enrichment, creates analytical outputs, analyzes returns, and writes final outputs to Parquet and CSV.

## Project Structure

```text
ecommerce-pyspark-etl-pipeline/
├── data/
│   └── data/
│       ├── orders.csv
│       ├── customers.csv
│       ├── order_items.csv
│       └── returns.csv
├── output/
│   ├── enriched_orders/
│   ├── orphaned_order_items/
│   ├── rejected/
│   └── summaries/
├── tests/
│   └── test_transformations.py
├── pipeline.py
├── requirements.txt
└── README.md
````

## Tech Stack

* Python
* PySpark
* pytest

## Setup Instructions

Create a virtual environment:

```bash
python -m venv sparkenv
```

Activate the virtual environment:

```bash
source sparkenv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Data

The pipeline expects the CSV files to be located in:

```text
data/data/
```

Required files:

```text
orders.csv
customers.csv
order_items.csv
returns.csv
```

## How to Run the Pipeline

Run:

```bash
python pipeline.py
```

The pipeline will:

1. Load the raw CSV files using explicit schemas.
2. Cast columns to correct data types.
3. Collect invalid cast records into rejected DataFrames.
4. Remove duplicate rows.
5. Normalize date columns.
6. Standardize customer tiers to lowercase.
7. Drop rows with missing key fields.
8. Flag negative order amounts.
9. Join orders, customers, and order items.
10. Isolate orphaned order items.
11. Create analytical outputs using window functions.
12. Analyze returns.
13. Write outputs to the `output/` folder.

## Outputs

The pipeline writes the following outputs:

```text
output/enriched_orders/
output/orphaned_order_items/
output/rejected/orders/
output/rejected/customers/
output/rejected/order_items/
output/rejected/returns/
output/summaries/customers_ranked/
output/summaries/rolling_7_day_order_count/
output/summaries/category_revenue_share/
output/summaries/returns_enriched/
output/summaries/return_rate_by_category/
output/summaries/return_rate_by_tier/
output/summaries/top_10_refund_customers/
```

The final enriched dataset is written to Parquet and partitioned by:

```text
order_year
order_month
```

All writes use:

```python
mode("overwrite")
```

This makes the pipeline idempotent.

## Tasks Completed

### Task 01: Data Ingestion and Schema Enforcement

* Loaded all four CSV files using explicit schemas.
* Avoided `inferSchema=True`.
* Casted date and numeric fields.
* Created rejected DataFrames for invalid cast records.

### Task 02: Data Quality and Cleaning

* Removed exact duplicate rows.
* Normalized dates.
* Standardized `customer_tier` to lowercase.
* Dropped rows with null `order_id` or `customer_id`.
* Flagged negative order amounts using `is_negative_amount`.

### Task 03: Joins and Enrichment

* Used an anti-join to isolate orphaned order items.
* Joined orders to customers.
* Joined orders/customers to order items.
* Created `net_amount`.

Formula:

```text
net_amount = total_amount * (1 - discount_pct / 100)
```

### Task 04: Aggregations and Window Functions

Created:

* Customers ranked by lifetime net spend within each country.
* 7-day rolling order count per customer.
* Product category revenue share per calendar month.

### Task 05: Return Analysis

Created:

* Returns joined to enriched orders.
* Return rate by category.
* Return rate by customer tier.
* Top 10 customers by refund amount.
* `refund_exceeds_order` anomaly flag.

### Task 06: Output and Partitioning

* Wrote final enriched dataset to Parquet.
* Partitioned final enriched dataset by year and month.
* Wrote summary tables to CSV.
* Used overwrite mode for all writes.

## Bonus: Unit Tests

This project includes pytest unit tests for selected cleaning and transformation logic.

The tests cover:

* Removing rows with null order/customer keys.
* Flagging negative order amounts without dropping them.
* Standardizing customer tier casing.
* Calculating net amount correctly.

Run the tests with:

```bash
pytest tests/test_transformations.py
```

Current test result:

```text
4 passed
```

## Assumptions

* The data files are stored in `data/data/`.
* The pipeline is designed to run locally using Spark local mode.
* Negative order amounts are flagged, not dropped.
* Rejected rows are written separately for auditability.
* The enriched dataset is at order-item level because orders are joined to order items.

## Known Limitations

* The pipeline runs locally and is not optimized for a distributed cluster.
* Return analysis joins returns to enriched order-item-level data, so some return-related summaries may contain duplicated rows if an order has multiple items. Distinct counts are used where needed to reduce overcounting.
* The project does not use an external database or cloud storage.

## Design Decisions

* Explicit schemas were used to avoid unreliable schema inference.
* Native Spark functions were used instead of Python UDFs.
* `left_anti` join was used to isolate orphaned order items.
* `inner` join was used between orders and customers because valid customer information is required for customer-level analytics.
* `left` join was used between orders and order items to keep valid orders even if item records are missing.
* Outputs use overwrite mode so the pipeline can be rerun safely.

## Note to self 
This work needs to be modularised, use separation of concerns and modules

