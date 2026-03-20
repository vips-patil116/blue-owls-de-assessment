# The Blue Owls Solutions — Azure Data Engineer Take-Home Assessment

**Mid-Level Role — Confidential**

| | |
|---|---|
| **Tech Stack** | Python, PySpark, SQL, Jupyter Lab |
| **Deliverable** | Public GitHub repository |
| **Data Source** | Blue Owls Data API (credentials provided separately) |

---

> **Before you begin:** Read [GETTING_STARTED.md](GETTING_STARTED.md) for environment setup instructions.

---

## Overview

You are a data engineer joining a retail analytics team at Blue Owls Solutions. A partner company has exposed their e-commerce data through a set of internal APIs. Your job is to build a data pipeline that ingests this data, transforms it through a medallion architecture, and produces a star schema for analytics.

The underlying data is based on the Olist Brazilian E-Commerce dataset and is served through our data API. The API is intentionally imperfect — it simulates real-world conditions including intermittent failures, authentication challenges, and occasional data quality issues.

---

## Data Access

The data is available through the Blue Owls Data API at `{base_url}/api/v1/data`.

**Authentication:** All requests require a Bearer token. Obtain one by calling the `/api/v1/auth/token` endpoint with the credentials provided in your welcome email.

### Available Endpoints

- `GET /api/v1/data/orders`
- `GET /api/v1/data/order_items`
- `GET /api/v1/data/customers`
- `GET /api/v1/data/products`
- `GET /api/v1/data/sellers`
- `GET /api/v1/data/payments`

Each endpoint supports pagination via `?page=1&page_size=1000`. Responses are returned as JSON arrays.

The `orders` and `order_items` endpoints also support date filtering via `?date_from=YYYY-MM-DD&date_to=YYYY-MM-DD`.

> **📅 Data scope:** Ingest records from **2018-07-01 onward** across all endpoints. Use the `date_from` parameter where supported, and apply the same cutoff when filtering related tables (e.g. payments, customers) by joining on order date.

> **⚠️ Important:** The API is intentionally imperfect. It may return intermittent 500 errors, token expiration responses (401), rate limiting (429), and occasional malformed or incomplete records in the payload. Your pipeline should handle these gracefully.

---

## Part 1 — Data Ingestion & Resilience (Weight: 30%)

Build an ingestion layer in Python that pulls data from all API endpoints. Your solution should:

- Handle authentication and automatic token refresh on 401 responses
- Implement retry logic with backoff for 500 and 429 errors
- Validate response payloads and log or quarantine malformed records
- Support paginated extraction across all endpoints
- Be idempotent — running it twice should not produce duplicate data

Save the raw ingested data as your Bronze layer (CSV files in a `bronze/` folder). Add an `_ingested_at` timestamp column and `_source_endpoint` column to each file.

**Data management strategy:** Bronze is append-only — each pipeline run adds new records without modifying or overwriting existing data. Your ingestion logic should ensure that re-running the pipeline does not create duplicates for that date ensuring historisation.

---

## Part 2 — Prescribed Star Schema (Weight: 10%)

Implement the following star schema as your Gold layer. The model is fully defined below — your job is to build it correctly through your transformation pipeline.

### Fact Table: `fact_order_items`

**Grain:** one row per item sold in an order

| Column | Type | Source / Logic |
|---|---|---|
| order_item_sk | integer | Surrogate key |
| order_id | string | orders |
| order_item_id | integer | order_items |
| customer_key | integer | FK → dim_customers |
| product_key | integer | FK → dim_products |
| seller_key | integer | FK → dim_sellers |
| order_date | date | orders (purchase_timestamp) |
| order_status | string | orders |
| price | decimal | order_items |
| freight_value | decimal | order_items |
| payment_value | decimal | Summed from payments for this order, distributed proportionally across items |
| payment_type | string | payments (primary payment method for the order) |
| payment_installments | integer | payments |
| days_to_deliver | integer | Calculated: delivered_customer_date − purchase_timestamp |
| days_delivery_vs_estimate | integer | Calculated: delivered_customer_date − estimated_delivery_date |
| is_late_delivery | boolean | True if days_delivery_vs_estimate > 0 |

### Dimension: `dim_customers`

**Grain:** one row per unique customer (deduplicated on customer_unique_id)

| Column | Type | Source / Logic |
|---|---|---|
| customer_key | integer | Surrogate key |
| customer_unique_id | string | customers (deduplicate on this, not customer_id) |
| customer_city | string | customers (most recent order's location) |
| customer_state | string | customers (most recent order's location) |
| customer_zip_code_prefix | string | customers |
| first_order_date | date | Derived from orders |
| total_orders | integer | Count of distinct orders |
| total_spend | decimal | Sum of price + freight from order_items |
| is_repeat_customer | boolean | True if total_orders > 1 |

### Dimension: `dim_products`

**Grain:** one row per product

| Column | Type | Source / Logic |
|---|---|---|
| product_key | integer | Surrogate key |
| product_id | string | products |
| product_category_name | string | products (handle nulls as "unknown") |
| product_weight_g | decimal | products |
| product_volume_cm3 | decimal | Calculated: length × height × width |
| product_photos_qty | integer | products |
| product_description_length | integer | products |

### Dimension: `dim_sellers`

**Grain:** one row per seller

| Column | Type | Source / Logic |
|---|---|---|
| seller_key | integer | Surrogate key |
| seller_id | string | sellers |
| seller_city | string | sellers |
| seller_state | string | sellers |
| seller_zip_code_prefix | string | sellers |

---

## Part 3 — PySpark Transformation Pipeline (Weight: 30%)

Implement the star schema above through a medallion architecture. Output each layer as CSVs in folders:

```
output/
├── bronze/       # Raw ingested data, one CSV per API endpoint
├── silver/       # Cleaned & typed data, one CSV per source table
└── gold/         # Star schema tables as defined in Part 2
```

### Bronze Layer

Save the raw API responses as-is, one CSV per endpoint. Add `_ingested_at` and `_source_endpoint` metadata columns.

### Silver Layer

Clean and standardize each source table individually:

- Handle nulls (document your strategy per field)
- Cast columns to correct types (dates as dates, decimals as decimals)
- Remove exact duplicate rows
- Flag records with data quality issues in a boolean `_is_valid` column (e.g., orders with delivery date before purchase date, negative prices, missing required foreign keys)

**Data management strategy:** Silver reflects the current state of each record — implement upsert logic using the natural key of each table so that re-processing updates existing records rather than duplicating them.

### Gold Layer

Build the four tables exactly as specified in Part 2. Requirements:

- All surrogate keys should be deterministic (reproducible across runs)
- All foreign key relationships should be valid — no orphan keys
- Include a brief validation step in your code that prints record counts per table and checks referential integrity

---

## Part 4 — SQL Analysis (Weight: 15%)

Write SQL queries against your Gold layer tables. These should be compatible with Spark SQL or T-SQL. For each query, include a brief comment explaining your approach.

### Query 1 — Revenue Trend Analysis with Ranking

For each product category, calculate monthly revenue (price + freight_value) and rank categories within each month by revenue. Then for the top 5 categories by overall revenue, show their month-over-month revenue growth percentage and a 3-month rolling average of revenue. Only include months where the category had at least 10 transactions.

**Expected output columns:** `product_category_name, year, month, monthly_revenue, monthly_rank, mom_growth_pct, rolling_3m_avg_revenue`

### Query 2 — Seller Performance Scorecard

Build a seller scorecard that ranks sellers on a composite score. For each seller, calculate late delivery rate (percentage of orders where `is_late_delivery = true`), average `days_delivery_vs_estimate`, total revenue, and order count. Compute a percentile rank for each metric across all sellers (higher is better — invert ranking for late delivery rate and `days_delivery_vs_estimate` so lower values get higher percentiles). Only include sellers with at least 20 orders. Create a `composite_score` as a weighted average: on-time delivery percentile 40%, delivery speed percentile 30%, revenue percentile 30%.

**Expected output columns:** `seller_id, seller_state, total_orders, total_revenue, late_delivery_rate, avg_days_vs_estimate, on_time_pctl, speed_pctl, revenue_pctl, composite_score, overall_rank`

---

## Part 5 — README & Technical Decisions (Weight: 15%)

Include a brief README (1–2 pages) covering:

- Your technical decisions and the reasoning behind them
- How you handled API failures and what your retry/resilience strategy is
- Assumptions you made and any trade-offs you weighed
- What you'd change or add for a production deployment on Azure/Fabric (scheduling, monitoring, CI/CD, security, cost optimization)

Be specific rather than generic. We value concrete reasoning over buzzwords.

---

## Submission Guidelines

1. Click **"Use this template"** → **"Create a new repository"** at the top of this page
2. Set your new repository to **Public** and give it a name (e.g. `blue-owls-de-assessment`)
3. Do all your work in your repository — commit regularly so we can follow your progress
4. When you are done, submit us the link to your public repository

Additional requirements:

- **Your entire pipeline must be implemented in Jupyter notebooks using PySpark** — see [GETTING_STARTED.md](GETTING_STARTED.md) for how to set up the environment
- Include a `requirements.txt` for any packages your code depends on beyond what the notebook image provides
- Notebooks should run end-to-end against the provided API without manual intervention
- Do not commit the raw dataset files — your pipeline should pull from the API

---

## Evaluation Criteria

We assess the following, roughly in order of importance:

- **Resilience and error handling** — how your code deals with the imperfect API
- **Pipeline correctness** — does the Gold layer match the prescribed schema with valid data?
- **PySpark code quality** — modular, readable, well-structured
- **SQL correctness and analytical depth**
- **Clarity of communication in the README**

We are not looking for perfection. We are looking for someone who builds resilient systems, thinks clearly about data, and can articulate their decisions.


---

*Good luck! We look forward to reviewing your work.*
