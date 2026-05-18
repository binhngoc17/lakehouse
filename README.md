# trino-demo

Banking datalake demo: Trino + Nessie (Iceberg REST) + MinIO.

## Quick start

```bash
docker compose build
docker compose up -d
docker compose run --rm --profile tools seed
docker compose run --rm --profile tools pipeline-runner
open http://localhost:3000
```

Silver tables: `iceberg.silver.{customers,accounts,transactions}`
Gold tables: `iceberg.gold.{customer_profile,monthly_spending,high_value_customers}`

Trino UI: http://localhost:8080
MinIO console: http://localhost:9001 (minioadmin / minioadmin)
Nessie UI: http://localhost:19120
