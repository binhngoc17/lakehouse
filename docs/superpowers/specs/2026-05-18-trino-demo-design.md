# Trino Demo — Design Spec

**Date:** 2026-05-18  
**Domain:** Banking (Customer360 + Transactions)  
**Goal:** Deployable demo of Trino as a query engine for a banking datalake: silver Iceberg tables → gold Iceberg tables via SQL-as-code pipelines, with a dark-themed data catalog UI.

**Two deployment modes (same images, same config):**

- `docker compose up` — local dev/test, single command, no cluster needed
- Helm chart — Kubernetes deployment for shared/production demo environments

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     Kubernetes Cluster                       │
│                                                              │
│  ┌──────────┐   ┌──────────────────┐   ┌─────────────────┐ │
│  │  MinIO   │◄──│      Trino       │◄──│Pipeline Runner  │ │
│  │  (S3)    │   │ Iceberg connector│   │  k8s CronJob    │ │
│  │ silver/  │   │ → Nessie catalog │   │ SQL+YAML defns  │ │
│  │   gold/  │   └────────┬─────────┘   └─────────────────┘ │
│  └──────────┘            │                                   │
│                  ┌───────▼────────┐   ┌──────────────────┐  │
│                  │ Project Nessie │   │   Demo UI (nginx) │  │
│                  │(Iceberg REST   │   │  Catalog · Canvas │  │
│                  │ catalog)       │   │  SQL runner       │  │
│                  └────────────────┘   └──────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

**Components:**

| Component       | Image                  | Role                                                                                         |
| --------------- | ---------------------- | -------------------------------------------------------------------------------------------- |
| MinIO           | `minio/minio`          | S3-compatible object store for Iceberg data files                                            |
| Project Nessie  | `projectnessie/nessie` | Iceberg REST catalog (git-like branching)                                                    |
| Trino           | `trinodb/trino`        | SQL query engine with Iceberg connector                                                      |
| Seed Job        | custom Python          | One-shot k8s Job: generates fake banking data → registers as Iceberg tables via PyIceberg    |
| Pipeline Runner | custom Python          | k8s CronJob: reads `pipelines/*.yaml`, executes SQL against Trino to materialize gold tables |
| Demo UI         | nginx + HTML           | Dark-themed single-file app (Data Catalog, Pipeline Canvas, SQL Runner, Insights)            |

Deployed as a single Helm umbrella chart. `trinodb/trino` and `bitnami/minio` are chart dependencies; Nessie is a custom template.

---

## Data Model

### Silver Layer (seeded Iceberg tables)

**`silver.customers`**

```
customer_id     VARCHAR    PK
full_name       VARCHAR
email           VARCHAR
phone           VARCHAR
segment         VARCHAR    (retail / premium / sme / corporate)
kyc_status      VARCHAR    (verified / pending / flagged)
join_date       DATE
birth_date      DATE
city            VARCHAR
country         VARCHAR    (ISO-3166)
```

**`silver.accounts`**

```
account_id      VARCHAR    PK
customer_id     VARCHAR    FK → customers
account_type    VARCHAR    (savings / checking / credit / loan)
balance         DECIMAL(18,2)
currency        VARCHAR    (ISO-4217, e.g. USD)
status          VARCHAR    (active / dormant / closed)
opened_date     DATE
credit_limit    DECIMAL(18,2)  nullable (credit accounts only)
```

**`silver.transactions`**

```
txn_id              VARCHAR    PK
account_id          VARCHAR    FK → accounts
amount              DECIMAL(18,2)
txn_type            VARCHAR    (debit / credit / transfer / fee)
merchant_category   VARCHAR    (groceries / travel / dining / utilities / …)
merchant_name       VARCHAR
channel             VARCHAR    (mobile / atm / branch / online / pos)
status              VARCHAR    (completed / pending / reversed)
ts                  TIMESTAMP
fx_rate             DECIMAL(10,6)  nullable (for non-base-currency txns)
```

Seed volume: 1,000 customers · 1,500 accounts · 50,000 transactions (realistic for a demo).

---

### Gold Layer (materialized by pipeline runner via Trino CTAS)

**`gold.customer_profile`** — Customer360 view

```
customer_id         VARCHAR
full_name           VARCHAR
segment             VARCHAR
kyc_status          VARCHAR
city                VARCHAR
total_accounts      BIGINT
total_balance       DECIMAL(18,2)
primary_currency    VARCHAR
total_txn_count     BIGINT
total_txn_amount    DECIMAL(18,2)
last_txn_date       DATE
preferred_channel   VARCHAR    (most frequent channel)
top_category        VARCHAR    (highest spend category)
customer_since_days BIGINT
```

**`gold.monthly_spending`** — Spending breakdown

```
customer_id         VARCHAR
year_month          VARCHAR    (e.g. 2025-03)
merchant_category   VARCHAR
total_amount        DECIMAL(18,2)
txn_count           BIGINT
```

**`gold.high_value_customers`** — Tier-1 customers

```
customer_id         VARCHAR
full_name           VARCHAR
segment             VARCHAR
total_balance       DECIMAL(18,2)
avg_monthly_spend   DECIMAL(18,2)
qualification_reason VARCHAR   (balance / spend / both)
```

---

## Pipeline Definitions (Data-as-Code)

Each pipeline has a `.sql` file (Trino SQL) + a `.yaml` sidecar.

### YAML contract

```yaml
name: customer_profile
description: "Customer360 — joins silver customers, accounts, and aggregated transactions"
schedule: "0 2 * * *" # cron: run at 2am daily
target: gold.customer_profile
strategy: replace # replace | append | merge
depends_on:
  - silver.customers
  - silver.accounts
  - silver.transactions
owner: data-team
tags: [customer360, gold]
```

### Pipeline Runner behavior

1. Reads all `pipelines/*.yaml` files
2. Topologically sorts by `depends_on` (silver tables are leaves)
3. For each pipeline, checks if silver source tables exist (Trino `SHOW TABLES`)
4. Executes the `.sql` with `CREATE OR REPLACE TABLE <target> AS (<sql>)` for `replace` strategy
5. Logs execution time and row count to stdout (picked up by k8s logs)

---

## Repo Structure

```
trino-demo/
├── helm/
│   └── trino-demo/
│       ├── Chart.yaml             # umbrella; deps: trinodb/trino, bitnami/minio
│       ├── values.yaml            # all tunable knobs in one file
│       └── templates/
│           ├── nessie.yaml        # Nessie Deployment + Service + ConfigMap
│           ├── seed-job.yaml      # k8s Job (runs once on install)
│           ├── pipeline-runner.yaml  # k8s CronJob
│           └── ui.yaml            # nginx Deployment + Service + ConfigMap
│
├── pipelines/
│   ├── customer_profile.sql
│   ├── customer_profile.yaml
│   ├── monthly_spending.sql
│   ├── monthly_spending.yaml
│   ├── high_value_customers.sql
│   └── high_value_customers.yaml
│
├── seed/
│   ├── generate.py                # Faker → PyIceberg → Nessie/MinIO
│   ├── requirements.txt
│   └── Dockerfile
│
├── pipeline-runner/
│   ├── run.py                     # reads pipelines/, calls Trino REST API
│   ├── requirements.txt
│   └── Dockerfile
│
└── ui/
    ├── index.html                 # self-contained dark-themed app
    └── nginx.conf
```

---

## Demo UI (WTG-style)

Five navigation views:

| Nav           | Content                                                                                                                                                  |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Sources**   | Silver table cards (customers, accounts, transactions) — row count, last seeded, schema summary                                                          |
| **Pipelines** | DAG canvas: silver nodes → transform node → gold nodes. Click any node → right panel shows `.sql` content + `.yaml` metadata                             |
| **Catalog**   | Table browser: select table → column list + 10-row sample queried live from Trino REST API                                                               |
| **Query**     | SQL editor → POST to Trino REST API → tabular results with execution time                                                                                |
| **Insights**  | Gold-layer KPIs: total customers, high-value count, daily transaction volume, top spending category (static data rendered from gold tables at page load) |

**Tech:** Pure HTML + vanilla JS + DM Sans/DM Mono fonts (same as wtg-demo.html). No framework. Trino REST API called directly from browser (CORS handled by nginx reverse-proxy to Trino coordinator).

---

## Helm values summary

```yaml
trino:
  coordinator:
    resources: { requests: { memory: 2Gi, cpu: 1 } }
  catalogs:
    iceberg: |
      connector.name=iceberg
      iceberg.catalog.type=rest
      iceberg.rest-catalog.uri=http://nessie:19120/api/v1
      iceberg.rest-catalog.warehouse=s3://datalake/
      fs.native-s3.enabled=true
      s3.endpoint=http://minio:9000
      s3.aws-access-key=minioadmin
      s3.aws-secret-key=minioadmin
      s3.path-style-access=true

minio:
  auth: { rootUser: minioadmin, rootPassword: minioadmin }
  defaultBuckets: datalake

nessie:
  image: projectnessie/nessie:0.80.0
  storage: InMemory # for demo; swap to JDBC for persistence

seed:
  customers: 1000
  accounts: 1500
  transactions: 50000

pipelineRunner:
  schedule: "0 2 * * *"

ui:
  trinoEndpoint: http://trino:8080
```

---

## Local Development (Docker Compose)

`docker-compose.yml` runs the exact same stack as Helm — same images, same env vars — so pipelines and UI can be developed and tested before deploying to Kubernetes.

```
docker compose up -d            # start MinIO, Nessie, Trino, UI
docker compose run seed         # generate silver data (idempotent)
docker compose run pipeline-runner  # materialize gold tables
open http://localhost:3000      # Demo UI
```

**Service map:**

| Service                    | Compose name      | Port                       |
| -------------------------- | ----------------- | -------------------------- |
| MinIO                      | `minio`           | 9000 (API), 9001 (console) |
| Project Nessie             | `nessie`          | 19120                      |
| Trino coordinator          | `trino`           | 8080                       |
| Demo UI (nginx)            | `ui`              | 3000                       |
| Seed (one-shot)            | `seed`            | —                          |
| Pipeline runner (one-shot) | `pipeline-runner` | —                          |

**Startup order:** MinIO and Nessie must be healthy before Trino starts (healthcheck deps). Seed runs after Trino is ready. Pipeline runner runs after seed.

**Repo addition:**

```
trino-demo/
├── docker-compose.yml          # local dev entry point
├── docker-compose.override.yml # optional: hot-reload mounts for seed/ and pipeline-runner/
└── ...
```

The `docker-compose.override.yml` mounts `./pipelines` and `./seed` as volumes so SQL/YAML edits are picked up without rebuilding images.

---

## Key Decisions

- **Nessie over Hive Metastore:** Simpler (no PostgreSQL dependency), provides git-like branch semantics which makes the silver→gold promotion story compelling in a demo.
- **PyIceberg for seeding:** Writes Iceberg tables directly without needing Spark; keeps the seed Job lightweight.
- **Single HTML file UI:** No build step, no Node.js, easy to inspect and modify. Trino REST API is proxied through nginx to avoid CORS issues.
- **`strategy: replace` for gold:** Simplest refresh semantics for a demo; `append` and `merge` are placeholders for future growth.
- **InMemory Nessie storage:** Eliminates the need for a Nessie database in demo mode; data is re-seeded on every install.
- **Docker Compose = Helm parity:** Same images and env vars in both environments means local testing is reliable — no "works on my machine" drift.
