# Blue Owls Assessment Submission

This submission implements the take-home as a notebook-first PySpark pipeline in [submission/blue_owls_assessment.ipynb](submission/blue_owls_assessment.ipynb), with SQL deliverables in [submission/sql/query_1.sql](submission/sql/query_1.sql) and [submission/sql/query_2.sql](submission/sql/query_2.sql).

## Technical Decisions

I kept the pipeline in a medallion structure with CSV outputs in Bronze, Silver, and Gold because that matches the assessment format and keeps the flow easy to inspect in a notebook. Bronze is append-only and idempotent through a manifest file that records completed page ingestions. Silver is a current-state layer built with latest-by-natural-key logic, exact deduplication, type casting, and `_is_valid` quality flags. Gold implements the prescribed star schema exactly, using deterministic `crc32` surrogate keys instead of sequence-based IDs so keys are reproducible across runs.

One important design choice was to keep Bronze close to the raw API output. `orders` and `order_items` use the API `date_from` filter because the API supports it. The other required endpoints do not, so they are ingested raw into Bronze and then reduced to the 2018-07-01 scope in Silver by joining back to qualifying orders and order items. That keeps Bronze faithful to the source while still applying the required business cutoff consistently.

## API Failures And Resilience

The ingestion client handles the failure patterns described in the assessment:

- automatic token refresh on `401`
- retry with backoff for `429` and `500`
- support for the `Retry-After` header on rate limits
- retry on transient request exceptions such as read timeouts
- basic payload validation before writing data
- malformed record quarantine for records missing their natural keys

This was implemented as a small reusable API client in the notebook so the extraction logic stays readable and rerunnable.

## Assumptions And Trade-Offs

I used only the six endpoints named in the assessment prompt: `orders`, `order_items`, `customers`, `products`, `sellers`, and `payments`. I did not include `reviews` or `geolocation` because they are not required for the requested star schema.

For null handling, I kept optional descriptive and operational fields as null unless the specification required a fallback value. The main exception is `product_category_name`, which is set to `"unknown"` in line with the Gold schema requirement. Invalid records are not dropped silently; they are marked with `_is_valid` in Silver so the pipeline can retain traceability while Gold uses only valid records.

I chose hash-based surrogate keys for determinism even though they are theoretically not collision-proof. In a production warehouse I would likely use a stronger hashing pattern or a managed key strategy, but for this assessment deterministic integer keys were the simplest reproducible option.

## Azure / Fabric Production Changes

For production deployment, I would move the data layers from CSV to Delta or Parquet on ADLS Gen2 or OneLake so that schema enforcement, incremental processing, and upserts are more reliable. I would orchestrate the notebook or equivalent jobs through Azure Data Factory, Fabric Data Factory, or Databricks Workflows, and replace the local JSON manifest with a small ingestion control table.

I would also add monitoring for API failures, row-count anomalies, and data quality checks, wire secrets through Key Vault or Fabric-managed connections, and separate dev/test/prod environments. For CI/CD, I would validate notebook execution in pull requests and publish to the target workspace through an automated deployment pipeline.
