# Demo Flow: Building a Simplified Data Warehouse with Trino

This walkthrough shows how the repo demonstrates a full data warehouse lifecycle —
raw data → structured silver layer → enriched gold layer — using only SQL, YAML,
and open-source tooling.

**Audience:** engineers, data engineers, or anyone evaluating Trino as a query
engine for a modern datalake.

**Duration:** ~15 minutes (most of which is waiting for services to start).

---

## 1 · The story in one slide

```
Raw source data
      │
      ▼
┌─────────────────────────────────────────┐
│  Silver layer  (Iceberg tables, MinIO)  │
│  customers · accounts · transactions    │
└───────────────────┬─────────────────────┘
                    │   Trino SQL pipelines
                    ▼
┌─────────────────────────────────────────┐
│  Gold layer   (Iceberg tables, MinIO)   │
│  customer_profile                       │
│  monthly_spending                       │
│  high_value_customers                   │
└───────────────────┬─────────────────────┘
                    │
                    ▼
              Demo UI — http://localhost:3000
              (live queries against Trino)
```

**Key message:** the entire transformation layer is defined as plain SQL files
with YAML metadata — no proprietary scheduler, no heavyweight orchestration
platform. Anyone who can write SQL can add or modify a pipeline.

---

## 2 · Start the stack

```bash
docker compose up -d
docker compose ps      # wait until all services are "healthy"
```

**What just started:**

| Service                  | Role                                                 |
| ------------------------ | ---------------------------------------------------- |
| **MinIO**                | S3-compatible object store — holds all Parquet files |
| **Iceberg REST catalog** | Tracks table schemas and file locations              |
| **Trino**                | Distributed SQL engine — the query brain             |
| **Demo UI**              | nginx serving the data catalog app at :3000          |

> Point out: there is no database server, no Hadoop cluster, no JVM-heavy
> metastore. Just object storage + a catalog + a query engine.

---

## 3 · Seed the silver layer

```bash
docker compose build seed
docker compose --profile tools run --rm seed
```

Expected output:

```
Generating data…
  created silver.customers: 1,000 rows
  created silver.accounts: 1,500 rows
  created silver.transactions: 50,000 rows
Seeding complete.
```

**What happened:**  
The seed script used PyIceberg to write three Iceberg tables directly to MinIO —
no Spark, no cluster, just Python. The Iceberg catalog now knows about three
tables in the `silver` namespace.

---

## 4 · Show the data in the UI — Sources view

Open **http://localhost:3000**

Navigate to **Sources**. The three silver cards load live row counts from Trino:

- `silver.customers` — 1,000 rows, 10 columns (customer_id, full_name, segment, kyc_status …)
- `silver.accounts` — 1,500 rows, 8 columns (account_id, customer_id, balance, currency …)
- `silver.transactions` — 50,000 rows, 10 columns (txn_id, amount, merchant_category, channel …)

> **Talking point:** the silver layer is intentionally simple — the raw facts,
> no aggregation. It maps directly to what a source system might produce.

---

## 5 · Explore the Catalog view

Navigate to **Catalog** → click `silver.customers`.

The UI runs two live Trino queries:

1. `DESCRIBE iceberg.silver.customers` — shows column names and types
2. `SELECT * FROM iceberg.silver.customers LIMIT 10` — shows sample rows

Repeat for `silver.transactions` to show the merchant_category, channel, and
txn_type columns that the pipelines will aggregate.

> **Talking point:** this is the same query any analyst would run in a BI tool
> — Trino exposes Iceberg tables as standard SQL tables.

---

## 6 · Show the pipeline definitions

Navigate to **Pipelines**.

The canvas shows the full DAG:

```
silver.customers   ──┐
silver.accounts    ──┼──► customer_profile pipeline ──► gold.customer_profile ──┐
silver.accounts    ──┤                                                            ├──► high_value_customers pipeline ──► gold.high_value_customers
silver.transactions──┘──► monthly_spending pipeline  ──► gold.monthly_spending ──┘
```

Click the **customer_profile** pipeline node. The right panel shows:

**YAML (metadata):**

```yaml
name: customer_profile
target: gold.customer_profile
strategy: replace
schedule: "0 2 * * *"
depends_on:
  - silver.customers
  - silver.accounts
  - silver.transactions
owner: data-team
```

**SQL (preview):**

```sql
WITH acct_agg AS (
    SELECT customer_id, COUNT(account_id) AS total_accounts,
           SUM(balance) AS total_balance ...
    FROM iceberg.silver.accounts WHERE status = 'active'
    GROUP BY customer_id
),
txn_agg AS ( ... ),
top_channel AS ( ... ),   -- most-used channel per customer
top_category AS ( ... )   -- highest-spend category per customer
SELECT c.customer_id, c.full_name, c.segment, ...
FROM iceberg.silver.customers c
LEFT JOIN acct_agg ...
LEFT JOIN txn_agg ...
```

> **Talking point:** this is data-as-code. The pipeline definition lives in
> `pipelines/customer_profile.yaml` and `pipelines/customer_profile.sql` in the
> repo. Version-controlled, reviewable in a PR, runnable locally.

Click **high_value_customers** — note that its `depends_on` lists
`gold.customer_profile` and `gold.monthly_spending`. The pipeline runner
automatically runs it last.

---

## 7 · Materialize the gold layer

```bash
docker compose build pipeline-runner
docker compose --profile tools run --rm pipeline-runner
```

Expected output:

```
Trino ready
Running 3 pipelines…
  ▶ customer_profile → iceberg.gold.customer_profile
    ✓ 1000 rows in 4.2s
  ▶ monthly_spending → iceberg.gold.monthly_spending
    ✓ 5989 rows in 3.1s
  ▶ high_value_customers → iceberg.gold.high_value_customers
    ✓ 473 rows in 2.8s
All pipelines complete.
```

**What happened:**  
The runner read all `pipelines/*.yaml`, resolved the dependency order
topologically, then executed each as:

```sql
CREATE OR REPLACE TABLE iceberg.gold.customer_profile AS
  <contents of customer_profile.sql>
```

Three new Iceberg tables now exist in MinIO under the `gold` namespace.

> **Talking point:** the strategy is `replace` — the whole table is refreshed
> on each run. Adding `append` or `merge` support is just extending the runner's
> SQL template, not a new tool.

---

## 8 · Query the gold layer live

Navigate to **Query** in the UI.

Run this Customer360 query (Cmd+Enter):

```sql
SELECT full_name, segment, total_balance,
       total_txn_count, preferred_channel, top_category
FROM iceberg.gold.customer_profile
ORDER BY total_balance DESC
LIMIT 10
```

Shows the top 10 customers by balance with their enriched profile — built from
three source tables in one SQL join.

Then run:

```sql
SELECT merchant_category,
       SUM(total_amount) AS total_spend,
       SUM(txn_count)    AS num_transactions
FROM iceberg.gold.monthly_spending
GROUP BY merchant_category
ORDER BY total_spend DESC
```

Shows where customers collectively spend the most money.

---

## 9 · Insights dashboard

Navigate to **Insights**.

The page loads four KPIs automatically:

| KPI                   | Value                        |
| --------------------- | ---------------------------- |
| Total customers       | 1,000                        |
| High-value customers  | ~473                         |
| Txn volume today      | (from live Trino query)      |
| Top spending category | (from gold.monthly_spending) |

Below the KPIs: a **spending category bar chart** and a **top 10 high-value
customers table** — both queried live from the gold layer.

> **Talking point:** a BI dashboard, a data catalog, and a SQL runner — all
> built on the same Trino REST API. No additional analytics server needed.

---

## 10 · Add a new pipeline (live demo option)

To show how easy it is to extend:

1. Create `pipelines/segment_summary.sql`:

```sql
SELECT
    segment,
    COUNT(DISTINCT customer_id)     AS num_customers,
    SUM(total_balance)              AS total_balance,
    AVG(total_txn_amount)           AS avg_txn_amount
FROM iceberg.gold.customer_profile
GROUP BY segment
ORDER BY total_balance DESC
```

2. Create `pipelines/segment_summary.yaml`:

```yaml
name: segment_summary
target: gold.segment_summary
strategy: replace
schedule: "0 2 * * *"
depends_on:
  - gold.customer_profile
owner: data-team
```

3. Run the pipeline runner again:

```bash
docker compose --profile tools run --rm pipeline-runner
```

The new `gold.segment_summary` table appears. Query it in the UI:

```sql
SELECT * FROM iceberg.gold.segment_summary
```

> **Talking point:** adding a pipeline is adding two files and running one
> command. No DAG editor, no YAML DSL, no redeployment of the scheduler.

---

## Summary

| Concept demonstrated    | How                                                            |
| ----------------------- | -------------------------------------------------------------- |
| Modern datalake storage | Iceberg tables on MinIO (S3)                                   |
| Lightweight catalog     | Iceberg REST catalog — no Hive metastore, no PostgreSQL        |
| SQL-as-code pipelines   | `pipelines/*.sql` + `*.yaml` — plain files, version-controlled |
| Dependency resolution   | Topological sort in ~50 lines of Python                        |
| Gold layer refresh      | `CREATE OR REPLACE TABLE ... AS SELECT` via Trino REST API     |
| Live data catalog       | Dark-themed SPA querying Trino directly from the browser       |
| Extensibility           | Add a pipeline = add 2 files                                   |
