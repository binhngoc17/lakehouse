# Trino Banking Datalake Demo

A deployable demo of [Trino](https://trino.io) as a query engine for a banking datalake:

- **Silver layer** — Iceberg tables (customers, accounts, transactions) seeded with synthetic banking data via PyIceberg
- **Gold layer** — materialized from silver via SQL-as-code pipelines (`pipelines/*.sql` + `*.yaml`)
- **Data catalog UI** — dark-themed SPA with Sources, Pipelines canvas, Catalog browser, SQL runner, and Insights dashboard

Two deployment modes using the same images and config:

- **Docker Compose** — local dev/demo, single command
- **Helm** — Kubernetes deployment

## Architecture

```
MinIO (S3) ◄── Trino (iceberg connector) ◄── Pipeline Runner (CronJob)
                       │
              Iceberg REST Catalog
              (tabulario/iceberg-rest)
                       │
              Demo UI (nginx :3000)
              Catalog · Canvas · SQL · Insights
```

| Service              | Image                    | Port                       |
| -------------------- | ------------------------ | -------------------------- |
| MinIO                | `minio/minio`            | 9000 (API), 9001 (console) |
| Iceberg REST catalog | `tabulario/iceberg-rest` | 8181 (mapped to 19120)     |
| Trino coordinator    | `trinodb/trino:435`      | 8080                       |
| Demo UI              | `nginx:1.27-alpine`      | 3000                       |

## Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- ~4 GB RAM available for Docker

## Local Setup (Docker Compose)

### 1. Clone and build

```bash
git clone <repo-url>
cd trino-demo
docker compose build seed pipeline-runner
```

This builds two images locally:

- `trino-demo-seed` — generates synthetic banking data and writes Iceberg tables
- `trino-demo-pipeline-runner` — reads `pipelines/*.yaml` and materializes gold tables via Trino

### 2. Start infrastructure

```bash
docker compose up -d
```

Starts MinIO, the Iceberg REST catalog, Trino, and the nginx UI. Services start in dependency order with healthchecks — Trino waits for the catalog to be ready before accepting queries.

Check all services are healthy:

```bash
docker compose ps
```

### 3. Seed silver tables

```bash
docker compose --profile tools run --rm seed
```

Generates and writes three Iceberg tables to MinIO via PyIceberg:

| Table                         | Rows   |
| ----------------------------- | ------ |
| `iceberg.silver.customers`    | 1,000  |
| `iceberg.silver.accounts`     | 1,500  |
| `iceberg.silver.transactions` | 50,000 |

Expected output:

```
Nessie ready
Generating data…
  created silver.customers: 1000 rows
  created silver.accounts: 1500 rows
  created silver.transactions: 50000 rows
Seeding complete.
```

### 4. Run pipelines

```bash
docker compose --profile tools run --rm pipeline-runner
```

Reads all `pipelines/*.yaml` definitions, topologically sorts them, and materializes three gold tables via `CREATE OR REPLACE TABLE ... AS SELECT`:

| Table                               | Description                                             |
| ----------------------------------- | ------------------------------------------------------- |
| `iceberg.gold.customer_profile`     | Customer360 — joins customers + accounts + transactions |
| `iceberg.gold.monthly_spending`     | Spend by category and month                             |
| `iceberg.gold.high_value_customers` | Tier-1 customers by balance or spend                    |

Expected output:

```
Trino ready
Running 3 pipelines…
  ▶ customer_profile → iceberg.gold.customer_profile
    ✓ 1000 rows in X.Xs
  ▶ monthly_spending → iceberg.gold.monthly_spending
    ✓ NNNN rows in X.Xs
  ▶ high_value_customers → iceberg.gold.high_value_customers
    ✓ NNN rows in X.Xs
All pipelines complete.
```

### 5. Open the demo UI

```bash
open http://localhost:3000
```

Five views:

| View          | What it shows                                                           |
| ------------- | ----------------------------------------------------------------------- |
| **Sources**   | Silver table cards with live row counts and last-seeded date            |
| **Pipelines** | Interactive DAG canvas — click any node for SQL + YAML                  |
| **Catalog**   | Table browser — schema and 10-row sample from Trino                     |
| **Query**     | Live SQL editor against `iceberg.*` (Cmd+Enter to run)                  |
| **Insights**  | Gold-layer KPIs, spending category bar chart, high-value customer table |

Other endpoints:

- Trino UI: http://localhost:8080
- MinIO console: http://localhost:9001 (minioadmin / minioadmin)

### Teardown

```bash
docker compose down          # stop containers, keep minio-data volume
docker compose down -v       # stop containers AND delete all data
```

## Data-as-Code Pipelines

Each pipeline is defined by two files in `pipelines/`:

```
pipelines/
  customer_profile.sql       # Trino SQL (the transformation)
  customer_profile.yaml      # metadata: target, schedule, depends_on, strategy
  monthly_spending.sql
  monthly_spending.yaml
  high_value_customers.sql
  high_value_customers.yaml
```

Example YAML:

```yaml
name: customer_profile
target: gold.customer_profile
strategy: replace # replace | append
schedule: "0 2 * * *"
depends_on:
  - silver.customers
  - silver.accounts
  - silver.transactions
owner: data-team
```

The pipeline runner resolves dependencies topologically, so `high_value_customers` (which reads from `gold.customer_profile` and `gold.monthly_spending`) always runs last.

## Development

### Hot-reload mounts

`docker-compose.override.yml` mounts `./pipelines` and `./seed` as volumes — SQL/YAML and Python edits are picked up without rebuilding:

```bash
docker compose -f docker-compose.yml -f docker-compose.override.yml up -d
```

### Run tests

```bash
# Seed tests
cd seed && pip install -r requirements.txt && pytest tests/ -v

# Pipeline runner tests
cd pipeline-runner && pip install -r requirements.txt && pytest tests/ -v
```

### Add a pipeline

1. Create `pipelines/<name>.sql` with a SELECT statement
2. Create `pipelines/<name>.yaml` with `name`, `target`, `depends_on`, `strategy`, `schedule`
3. Re-run `docker compose --profile tools run --rm pipeline-runner`

## Kubernetes (Helm)

The `helm/trino-demo/` umbrella chart deploys the full stack. Chart dependencies: `trinodb/trino` and `bitnami/minio`.

### Prerequisites

```bash
helm repo add trinodb https://trinodb.github.io/charts
helm repo add bitnami https://charts.bitnami.com/bitnami
helm dependency build helm/trino-demo/
```

### Build and push images

```bash
# Build
docker build -t <registry>/trino-demo-seed:latest seed/
docker build -t <registry>/trino-demo-pipeline-runner:latest pipeline-runner/

# Push
docker push <registry>/trino-demo-seed:latest
docker push <registry>/trino-demo-pipeline-runner:latest
```

### Install

```bash
helm install trino-demo helm/trino-demo/ \
  --set seed.image=<registry>/trino-demo-seed:latest \
  --set pipelineRunner.image=<registry>/trino-demo-pipeline-runner:latest
```

> **Note:** Install the chart with release name `trino-demo` — the Iceberg catalog config hardcodes `trino-demo-nessie` and `trino-demo-minio` as service hostnames.

The seed job runs automatically as a post-install hook. The pipeline runner is a CronJob scheduled at `0 2 * * *` by default; trigger it manually with:

```bash
kubectl create job --from=cronjob/trino-demo-pipeline-runner trino-demo-pipeline-runner-manual
```

Access the UI:

```bash
kubectl port-forward svc/trino-demo-ui 3000:3000
open http://localhost:3000
```

### Key values

| Key                         | Default     | Description                                 |
| --------------------------- | ----------- | ------------------------------------------- |
| `seed.image`                | `""`        | Your built seed image (required)            |
| `pipelineRunner.image`      | `""`        | Your built pipeline-runner image (required) |
| `pipelineRunner.schedule`   | `0 2 * * *` | Cron schedule for gold table refresh        |
| `seed.numCustomers`         | `1000`      | Rows to seed                                |
| `seed.numTransactions`      | `50000`     | Rows to seed                                |
| `minio.persistence.enabled` | `false`     | Enable for persistent storage               |
