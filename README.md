# Blue Owls Azure Data Engineer Assessment Solution

This repository contains a complete notebook-first submission for the Blue Owls Azure Data Engineer take-home.

## Deliverables

- Notebook pipeline: [submission/blue_owls_assessment.ipynb](/home/vipul/Tutorial/de-project/repo/submission/blue_owls_assessment.ipynb)
- SQL queries:
  - [submission/sql/query_1.sql](/home/vipul/Tutorial/de-project/repo/submission/sql/query_1.sql)
  - [submission/sql/query_2.sql](/home/vipul/Tutorial/de-project/repo/submission/sql/query_2.sql)
- Output data:
  - [submission/output/bronze](/home/vipul/Tutorial/de-project/repo/submission/output/bronze)
  - [submission/output/silver](/home/vipul/Tutorial/de-project/repo/submission/output/silver)
  - [submission/output/gold](/home/vipul/Tutorial/de-project/repo/submission/output/gold)
- Extra package requirements: [requirements.txt](/home/vipul/Tutorial/de-project/repo/requirements.txt)

## What The Notebook Does

The notebook pulls the six assessment endpoints from the mock API, keeps Bronze append-only, builds current-state Silver tables, and produces the prescribed Gold star schema.

- Bronze writes one CSV per endpoint with `_ingested_at` and `_source_endpoint`.
- Bronze is stored as raw endpoint output for all six required endpoints. The 2018-07-01 business scope is enforced downstream in Silver and Gold, not by pre-filtering Bronze rows.
- Bronze idempotency is handled with [submission/output/bronze/_manifest.json](/home/vipul/Tutorial/de-project/repo/submission/output/bronze/_manifest.json), which stores successfully ingested page signatures so reruns skip already completed pages.
- Silver removes exact duplicates, casts data types, flags invalid records in `_is_valid`, and upserts the latest version of each natural key using the most recent ingestion timestamp.
- Gold builds `dim_customers`, `dim_products`, `dim_sellers`, and `fact_order_items` with deterministic surrogate keys based on `crc32(...)`.

## Technical Decisions

The first design choice was to keep Bronze faithful to the raw endpoint outputs and move business scoping into Silver. `orders` and `order_items` use the API `date_from` filter because the assessment explicitly allows that. The other four required endpoints do not support a date filter, so Bronze ingests them raw and Silver reduces them to the 2018-07-01 scope by joining back to qualifying orders and order items. That stays closer to the stated Bronze requirement while still applying the same business cutoff consistently across related tables.

For resilience, the notebook wraps all data calls in a small API client that:

- authenticates once and automatically refreshes on `401`
- retries `429` and `500` responses with exponential backoff
- retries transient request exceptions such as read timeouts
- honors `Retry-After` when the API provides it
- validates the basic response shape before writing anything
- quarantines malformed records with missing natural keys under `output/bronze/quarantine/`

I chose a manifest-backed Bronze design rather than record-level deduplication in raw files. The assessment explicitly asks for append-only Bronze, so the manifest gives a clean operational contract: once a page is marked successful, reruns do not append it again. Then Silver is responsible for the current-state view through latest-by-natural-key upserts.

The Gold model follows the prescribed schema exactly. Payments are aggregated to the order level, the dominant `payment_type` is chosen by highest `payment_value` with alphabetical tie-break, and total `payment_value` is allocated across order items proportionally by item `price`. Customer dimension rows are deduplicated on `customer_unique_id`, with location attributes taken from the most recent order and the first order date taken from the earliest purchase.

## Validation Results

The notebook includes a simple validation section and completed successfully on March 27, 2026.

- `dim_customers`: 12,647 rows
- `dim_products`: 7,474 rows
- `dim_sellers`: 1,628 rows
- `fact_order_items`: 14,341 rows
- orphan foreign keys in the fact table: `0` for `customer_key`, `product_key`, and `seller_key`

Materialized output row counts:

- Bronze: `orders` 12,824, `order_items` 15,590, `customers` 99,441, `products` 32,951, `sellers` 3,095, `payments` 103,886
- Silver: `orders` 12,824, `order_items` 15,590, `customers` 12,824, `products` 7,474, `sellers` 1,628, `payments` 13,225
- Gold: `dim_customers` 12,647, `dim_products` 7,474, `dim_sellers` 1,628, `fact_order_items` 14,341

## Null Handling Strategy

The assessment asked for null handling to be documented, so the Silver layer uses the following table-level rules:

- `orders`: keep nullable operational timestamps as null, require `order_id`, `customer_id`, and `order_purchase_timestamp`, and mark a row invalid if `order_delivered_customer_date` is earlier than purchase time.
- `order_items`: keep `shipping_limit_date` nullable, require `order_id`, `order_item_id`, `product_id`, and `seller_id`, keep numeric nulls as null, and mark a row invalid if `price` or `freight_value` is negative or if there is no matching scoped order.
- `customers`: keep nullable descriptive fields as null, require `customer_id` and `customer_unique_id`, and derive the final customer location attributes in Gold from the most recent order for that customer.
- `products`: replace null `product_category_name` with `"unknown"` because Gold requires a usable category value, keep the dimensional measurements nullable, and therefore let `product_volume_cm3` remain null whenever any contributing dimension is missing.
- `sellers`: keep nullable descriptive fields as null and require only `seller_id`.
- `payments`: keep nullable `payment_type` as null, keep numeric nulls as null after casting, and mark a row invalid if `order_id` or `payment_sequential` is missing or if `payment_value` is negative.

## Assumptions And Trade-Offs

- I used the six endpoints named in the assessment prompt and did not include `reviews` or `geolocation` in the pipeline because they are not required by the prescribed Gold schema.
- Bronze quarantine is keyed off malformed payload shape and missing natural keys. More advanced schema-level validation could be added, but I kept Bronze close to raw ingestion and pushed business validation into Silver.
- Surrogate keys use deterministic hashes instead of sequences. That satisfies reproducibility across runs, though like any hash-based integer key it is theoretically not collision-proof.
- Silver current-state logic uses the latest `_ingested_at` row for a natural key. In a production system I would prefer a source-side updated timestamp or CDC feed over ingestion time ordering.
- The template repository still includes the local mock API seed CSVs under `mock-api/data`. If Blue Owls interprets “do not commit raw dataset files” literally for the final public repo, those template files should be removed or clarified before submission.

## How To Run

1. Start the provided environment with `docker-compose up`.
2. Open Jupyter Lab at `http://localhost:8888`.
3. Run [submission/blue_owls_assessment.ipynb](/home/vipul/Tutorial/de-project/repo/submission/blue_owls_assessment.ipynb) from top to bottom.
4. Review the generated CSVs under [submission/output](/home/vipul/Tutorial/de-project/repo/submission/output).

## What I Would Do In Azure Or Microsoft Fabric

For production, I would move Bronze/Silver/Gold storage to ADLS Gen2 or OneLake and switch the file format from CSV to Delta or Parquet for schema enforcement, partition pruning, and efficient upserts. I would orchestrate the workflow with Azure Data Factory, Fabric Data Factory, or Databricks Workflows, then persist ingestion state in a small control table instead of a JSON manifest. Monitoring would include pipeline-level alerts, API error metrics, row-count anomaly checks, and data-quality rules wired into Azure Monitor or Fabric monitoring.

For CI/CD, I would add notebook validation in pull requests, parameterize environments through Key Vault or Fabric connections, and separate dev/test/prod storage accounts or workspaces. Security-wise, I would replace hard-coded credentials with managed identity or service principals, restrict storage access by role, and rotate secrets centrally. For cost control, I would favor incremental loads, compacted columnar storage, autoscaling compute, and a clear retention policy on Bronze snapshots and quarantine data.
