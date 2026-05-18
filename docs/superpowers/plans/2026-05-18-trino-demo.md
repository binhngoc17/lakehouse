# Trino Banking Datalake Demo — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A fully runnable banking datalake demo — `docker compose up` seeds silver Iceberg tables via PyIceberg, the pipeline runner materializes gold tables via Trino SQL-as-code, and a dark-themed HTML catalog lets you browse tables, view pipelines, run SQL, and see KPIs.

**Architecture:** Trino (Iceberg connector) → Project Nessie (Iceberg REST catalog) → MinIO (S3). Silver tables seeded by a Python init container. Gold tables created by a pipeline runner that reads `pipelines/*.yaml` + `*.sql` and issues `CREATE OR REPLACE TABLE … AS` via Trino REST API.

**Tech Stack:** Python 3.12, PyIceberg 0.7, PyArrow 16, Faker, Trino 435, Project Nessie 0.80, MinIO, nginx, Docker Compose, Helm 3

---

## File Map

```
trino-demo/
├── .gitignore
├── README.md
├── docker-compose.yml
├── docker-compose.override.yml
├── trino-conf/
│   ├── config.properties
│   └── iceberg.properties
├── pipelines/
│   ├── customer_profile.sql + .yaml
│   ├── monthly_spending.sql + .yaml
│   └── high_value_customers.sql + .yaml
├── seed/
│   ├── generate.py            — data generation + Iceberg write
│   ├── requirements.txt
│   ├── Dockerfile
│   └── tests/test_generate.py
├── pipeline-runner/
│   ├── run.py                 — YAML loader, topo sort, Trino REST client
│   ├── requirements.txt
│   ├── Dockerfile
│   └── tests/test_runner.py
├── ui/
│   ├── index.html             — WTG-style SPA (5 views)
│   └── nginx.conf
└── helm/trino-demo/
    ├── Chart.yaml
    ├── values.yaml
    └── templates/
        ├── nessie.yaml
        ├── seed-job.yaml
        ├── pipeline-runner.yaml
        └── ui.yaml
```

---

## Task 1: Repo scaffold

**Files:**

- Create: `.gitignore`
- Create: `README.md`
- Create: all directories

- [ ] **Step 1: Create directory structure**

```bash
cd ~/src/trino-demo
mkdir -p trino-conf pipelines seed/tests pipeline-runner/tests ui helm/trino-demo/templates
```

- [ ] **Step 2: Write `.gitignore`**

```
__pycache__/
*.pyc
.venv/
.env
*.egg-info/
helm/trino-demo/charts/
helm/trino-demo/Chart.lock
minio-data/
```

- [ ] **Step 3: Write `README.md`**

````markdown
# trino-demo

Banking datalake demo: Trino + Nessie (Iceberg REST) + MinIO.

## Quick start

```bash
docker compose build
docker compose up -d
docker compose run --rm seed
docker compose run --rm pipeline-runner
open http://localhost:3000
```
````

Silver tables: `iceberg.silver.{customers,accounts,transactions}`
Gold tables: `iceberg.gold.{customer_profile,monthly_spending,high_value_customers}`

Trino UI: http://localhost:8080
MinIO console: http://localhost:9001 (minioadmin / minioadmin)
Nessie UI: http://localhost:19120

````

- [ ] **Step 4: Commit**

```bash
git add .gitignore README.md
git commit -m "feat: repo scaffold"
````

---

## Task 2: Trino config files

**Files:**

- Create: `trino-conf/config.properties`
- Create: `trino-conf/iceberg.properties`

- [ ] **Step 1: Write `trino-conf/config.properties`**

```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=1GB
query.max-memory-per-node=512MB
discovery.uri=http://localhost:8080
```

- [ ] **Step 2: Write `trino-conf/iceberg.properties`**

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://nessie:19120/iceberg
iceberg.rest-catalog.warehouse=s3://datalake/
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.path-style-access=true
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
s3.region=us-east-1
```

- [ ] **Step 3: Commit**

```bash
git add trino-conf/
git commit -m "feat: trino catalog config for iceberg+nessie+minio"
```

---

## Task 3: Docker Compose

**Files:**

- Create: `docker-compose.yml`
- Create: `docker-compose.override.yml`

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
version: "3.9"

services:
  minio:
    image: minio/minio:RELEASE.2024-11-07T00-52-20Z
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 5s
      timeout: 5s
      retries: 15

  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
        mc alias set local http://minio:9000 minioadmin minioadmin &&
        mc mb --ignore-existing local/datalake &&
        echo 'bucket ready'
      "

  nessie:
    image: projectnessie/nessie:0.80.0
    environment:
      QUARKUS_PROFILE: prod
      NESSIE_VERSION_STORE_TYPE: IN_MEMORY
    ports:
      - "19120:19120"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:19120/api/v2/config"]
      interval: 5s
      timeout: 5s
      retries: 20

  trino:
    image: trinodb/trino:435
    depends_on:
      minio-init:
        condition: service_completed_successfully
      nessie:
        condition: service_healthy
    ports:
      - "8080:8080"
    volumes:
      - ./trino-conf/config.properties:/etc/trino/config.properties:ro
      - ./trino-conf/iceberg.properties:/etc/trino/catalog/iceberg.properties:ro
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/v1/info"]
      interval: 5s
      timeout: 5s
      retries: 30

  seed:
    build: ./seed
    depends_on:
      trino:
        condition: service_healthy
    environment:
      MINIO_ENDPOINT: http://minio:9000
      NESSIE_URI: http://nessie:19120/iceberg
      NUM_CUSTOMERS: "1000"
      NUM_ACCOUNTS: "1500"
      NUM_TRANSACTIONS: "50000"
    profiles:
      - tools

  pipeline-runner:
    build: ./pipeline-runner
    depends_on:
      trino:
        condition: service_healthy
    environment:
      TRINO_HOST: trino
      TRINO_PORT: "8080"
      PIPELINES_DIR: /pipelines
    volumes:
      - ./pipelines:/pipelines:ro
    profiles:
      - tools

  ui:
    image: nginx:1.27-alpine
    depends_on:
      trino:
        condition: service_healthy
    ports:
      - "3000:3000"
    volumes:
      - ./ui/index.html:/usr/share/nginx/html/index.html:ro
      - ./ui/nginx.conf:/etc/nginx/conf.d/default.conf:ro

volumes:
  minio-data:
```

- [ ] **Step 2: Write `docker-compose.override.yml`** (hot-reload mounts for development)

```yaml
version: "3.9"

services:
  seed:
    volumes:
      - ./seed/generate.py:/app/generate.py:ro
    profiles:
      - tools

  pipeline-runner:
    volumes:
      - ./pipeline-runner/run.py:/app/run.py:ro
      - ./pipelines:/pipelines:ro
    profiles:
      - tools
```

- [ ] **Step 3: Verify compose parses without error**

```bash
docker compose config --quiet
```

Expected: no output (silent = valid)

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml docker-compose.override.yml
git commit -m "feat: docker compose stack (minio, nessie, trino, ui)"
```

---

## Task 4: Seed data generator — tests first

**Files:**

- Create: `seed/tests/test_generate.py`
- Create: `seed/generate.py`
- Create: `seed/requirements.txt`
- Create: `seed/Dockerfile`

- [ ] **Step 1: Write `seed/requirements.txt`**

```
faker==24.11.0
pyiceberg[s3fs]==0.7.1
pyarrow==16.1.0
boto3==1.34.131
pytest==8.2.0
```

- [ ] **Step 2: Write failing tests in `seed/tests/test_generate.py`**

```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from generate import (
    generate_customers,
    generate_accounts,
    generate_transactions,
)


def test_customers_count():
    customers = generate_customers(10)
    assert len(customers) == 10


def test_customers_schema():
    customers = generate_customers(5)
    required = {"customer_id", "full_name", "email", "phone", "segment",
                "kyc_status", "join_date", "birth_date", "city", "country"}
    for c in customers:
        assert required == set(c.keys())


def test_customers_ids_unique():
    customers = generate_customers(100)
    ids = [c["customer_id"] for c in customers]
    assert len(ids) == len(set(ids))


def test_accounts_reference_customers():
    customers = generate_customers(20)
    accounts = generate_accounts(customers, 30)
    cids = {c["customer_id"] for c in customers}
    for a in accounts:
        assert a["customer_id"] in cids


def test_accounts_credit_limit_only_for_credit():
    customers = generate_customers(50)
    accounts = generate_accounts(customers, 100)
    for a in accounts:
        if a["account_type"] != "credit":
            assert a["credit_limit"] is None


def test_transactions_reference_active_accounts():
    customers = generate_customers(20)
    accounts = generate_accounts(customers, 30)
    txns = generate_transactions(accounts, 50)
    active_ids = {a["account_id"] for a in accounts if a["status"] == "active"}
    for t in txns:
        assert t["account_id"] in active_ids


def test_transactions_amounts_positive():
    customers = generate_customers(10)
    accounts = generate_accounts(customers, 15)
    txns = generate_transactions(accounts, 30)
    for t in txns:
        assert t["amount"] > 0
```

- [ ] **Step 3: Run tests — expect failures (generate.py doesn't exist yet)**

```bash
cd ~/src/trino-demo/seed
python -m pytest tests/test_generate.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'generate'`

- [ ] **Step 4: Write `seed/generate.py`**

```python
import os
import time
import random
import urllib.request
from datetime import date, timedelta, datetime

import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, DecimalType, DateType, TimestampType,
)

fake = Faker()
random.seed(42)
Faker.seed(42)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/iceberg")
NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", "1000"))
NUM_ACCOUNTS = int(os.getenv("NUM_ACCOUNTS", "1500"))
NUM_TRANSACTIONS = int(os.getenv("NUM_TRANSACTIONS", "50000"))

_SEGMENTS = ["retail", "premium", "sme", "corporate"]
_KYC = ["verified", "pending", "flagged"]
_ACCT_TYPES = ["savings", "checking", "credit", "loan"]
_TXN_TYPES = ["debit", "credit", "transfer", "fee"]
_CATEGORIES = ["groceries", "travel", "dining", "utilities",
               "entertainment", "healthcare", "fuel", "shopping"]
_CHANNELS = ["mobile", "atm", "branch", "online", "pos"]
_STATUSES = ["active", "active", "active", "dormant", "closed"]


def generate_customers(n: int) -> list[dict]:
    return [
        {
            "customer_id": f"cust_{i:06d}",
            "full_name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number()[:20],
            "segment": random.choice(_SEGMENTS),
            "kyc_status": random.choice(_KYC),
            "join_date": fake.date_between(start_date="-10y", end_date="today"),
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=75),
            "city": fake.city(),
            "country": fake.country_code(),
        }
        for i in range(n)
    ]


def generate_accounts(customers: list[dict], n: int) -> list[dict]:
    accounts = []
    for i in range(n):
        cust = random.choice(customers)
        acct_type = random.choice(_ACCT_TYPES)
        accounts.append(
            {
                "account_id": f"acct_{i:06d}",
                "customer_id": cust["customer_id"],
                "account_type": acct_type,
                "balance": round(random.uniform(100, 200_000), 2),
                "currency": random.choice(["USD", "EUR", "GBP", "SGD"]),
                "status": random.choice(_STATUSES),
                "opened_date": fake.date_between(
                    start_date=cust["join_date"], end_date="today"
                ),
                "credit_limit": round(random.uniform(1000, 50_000), 2)
                if acct_type == "credit"
                else None,
            }
        )
    return accounts


def generate_transactions(accounts: list[dict], n: int) -> list[dict]:
    active = [a for a in accounts if a["status"] == "active"]
    base_ts = datetime(2024, 1, 1)
    txns = []
    for i in range(n):
        acct = random.choice(active)
        ts = base_ts + timedelta(
            days=random.randint(0, 500),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        txns.append(
            {
                "txn_id": f"txn_{i:08d}",
                "account_id": acct["account_id"],
                "amount": round(random.uniform(1, 5_000), 2),
                "txn_type": random.choice(_TXN_TYPES),
                "merchant_category": random.choice(_CATEGORIES),
                "merchant_name": fake.company()[:60],
                "channel": random.choice(_CHANNELS),
                "status": random.choice(["completed", "completed", "pending", "reversed"]),
                "ts": ts,
                "fx_rate": round(random.uniform(0.8, 1.5), 6)
                if random.random() < 0.2
                else None,
            }
        )
    return txns


# ── PyArrow schemas ────────────────────────────────────────────────────────────

CUSTOMER_ARROW = pa.schema([
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("full_name", pa.string()),
    pa.field("email", pa.string()),
    pa.field("phone", pa.string()),
    pa.field("segment", pa.string()),
    pa.field("kyc_status", pa.string()),
    pa.field("join_date", pa.date32()),
    pa.field("birth_date", pa.date32()),
    pa.field("city", pa.string()),
    pa.field("country", pa.string()),
])

ACCOUNT_ARROW = pa.schema([
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("customer_id", pa.string()),
    pa.field("account_type", pa.string()),
    pa.field("balance", pa.decimal128(18, 2)),
    pa.field("currency", pa.string()),
    pa.field("status", pa.string()),
    pa.field("opened_date", pa.date32()),
    pa.field("credit_limit", pa.decimal128(18, 2)),
])

TRANSACTION_ARROW = pa.schema([
    pa.field("txn_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string()),
    pa.field("amount", pa.decimal128(18, 2)),
    pa.field("txn_type", pa.string()),
    pa.field("merchant_category", pa.string()),
    pa.field("merchant_name", pa.string()),
    pa.field("channel", pa.string()),
    pa.field("status", pa.string()),
    pa.field("ts", pa.timestamp("us")),
    pa.field("fx_rate", pa.decimal128(10, 6)),
])


def _to_decimal(val, precision=18, scale=2):
    if val is None:
        return None
    from decimal import Decimal, ROUND_HALF_UP
    fmt = "." + "0" * scale
    return Decimal(str(val)).quantize(Decimal(fmt), rounding=ROUND_HALF_UP)


def _customers_to_arrow(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=CUSTOMER_ARROW)


def _accounts_to_arrow(rows: list[dict]) -> pa.Table:
    coerced = [
        {**r,
         "balance": _to_decimal(r["balance"]),
         "credit_limit": _to_decimal(r["credit_limit"])}
        for r in rows
    ]
    return pa.Table.from_pylist(coerced, schema=ACCOUNT_ARROW)


def _transactions_to_arrow(rows: list[dict]) -> pa.Table:
    coerced = [
        {**r,
         "amount": _to_decimal(r["amount"]),
         "fx_rate": _to_decimal(r["fx_rate"], precision=10, scale=6)}
        for r in rows
    ]
    return pa.Table.from_pylist(coerced, schema=TRANSACTION_ARROW)


# ── Catalog helpers ────────────────────────────────────────────────────────────

def _wait_for_nessie(uri: str, retries: int = 30, delay: int = 3) -> None:
    health = uri.rstrip("/").replace("/iceberg", "") + "/api/v2/config"
    for i in range(retries):
        try:
            urllib.request.urlopen(health, timeout=3)
            print(f"Nessie ready")
            return
        except Exception:
            print(f"Waiting for Nessie ({i + 1}/{retries})…")
            time.sleep(delay)
    raise RuntimeError("Nessie did not become ready")


def _load_catalog():
    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": NESSIE_URI,
            "warehouse": "s3://datalake/",
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
        },
    )


def seed(catalog=None) -> None:
    _wait_for_nessie(NESSIE_URI)
    if catalog is None:
        catalog = _load_catalog()

    # namespaces
    for ns in ("silver",):
        try:
            catalog.create_namespace(ns)
        except Exception:
            pass

    print("Generating data…")
    customers = generate_customers(NUM_CUSTOMERS)
    accounts = generate_accounts(customers, NUM_ACCOUNTS)
    transactions = generate_transactions(accounts, NUM_TRANSACTIONS)

    _write_table(catalog, "silver.customers", _customers_to_arrow(customers))
    _write_table(catalog, "silver.accounts", _accounts_to_arrow(accounts))
    _write_table(catalog, "silver.transactions", _transactions_to_arrow(transactions))
    print("Seeding complete.")


def _write_table(catalog, identifier: str, arrow_table: pa.Table) -> None:
    try:
        tbl = catalog.load_table(identifier)
        tbl.overwrite(arrow_table)
        print(f"  overwritten {identifier}: {len(arrow_table)} rows")
    except Exception:
        tbl = catalog.create_table(identifier, schema=arrow_table.schema)
        tbl.append(arrow_table)
        print(f"  created {identifier}: {len(arrow_table)} rows")


if __name__ == "__main__":
    seed()
```

- [ ] **Step 5: Run tests — expect pass**

```bash
cd ~/src/trino-demo/seed
pip install -r requirements.txt -q
python -m pytest tests/test_generate.py -v
```

Expected:

```
PASSED tests/test_generate.py::test_customers_count
PASSED tests/test_generate.py::test_customers_schema
PASSED tests/test_generate.py::test_customers_ids_unique
PASSED tests/test_generate.py::test_accounts_reference_customers
PASSED tests/test_generate.py::test_accounts_credit_limit_only_for_credit
PASSED tests/test_generate.py::test_transactions_reference_active_accounts
PASSED tests/test_generate.py::test_transactions_amounts_positive
```

- [ ] **Step 6: Write `seed/Dockerfile`**

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY generate.py .
CMD ["python", "generate.py"]
```

- [ ] **Step 7: Commit**

```bash
git add seed/
git commit -m "feat: seed data generator with PyIceberg + tests"
```

---

## Task 5: Pipeline SQL + YAML definitions

**Files:**

- Create: `pipelines/customer_profile.{sql,yaml}`
- Create: `pipelines/monthly_spending.{sql,yaml}`
- Create: `pipelines/high_value_customers.{sql,yaml}`

- [ ] **Step 1: Write `pipelines/customer_profile.yaml`**

```yaml
name: customer_profile
description: "Customer360 — joins silver customers, accounts, and aggregated transactions"
schedule: "0 2 * * *"
target: gold.customer_profile
strategy: replace
depends_on:
  - silver.customers
  - silver.accounts
  - silver.transactions
owner: data-team
tags: [customer360, gold]
```

- [ ] **Step 2: Write `pipelines/customer_profile.sql`**

```sql
WITH acct_agg AS (
    SELECT
        customer_id,
        COUNT(account_id)                          AS total_accounts,
        SUM(balance)                               AS total_balance,
        MAX(currency)                              AS primary_currency
    FROM iceberg.silver.accounts
    WHERE status = 'active'
    GROUP BY customer_id
),
txn_agg AS (
    SELECT
        a.customer_id,
        COUNT(t.txn_id)                            AS total_txn_count,
        SUM(t.amount)                              AS total_txn_amount,
        MAX(CAST(t.ts AS DATE))                    AS last_txn_date
    FROM iceberg.silver.accounts a
    JOIN iceberg.silver.transactions t
        ON a.account_id = t.account_id AND t.status = 'completed'
    GROUP BY a.customer_id
),
top_channel AS (
    SELECT customer_id, channel AS preferred_channel
    FROM (
        SELECT
            a.customer_id,
            t.channel,
            ROW_NUMBER() OVER (
                PARTITION BY a.customer_id ORDER BY COUNT(*) DESC
            ) AS rn
        FROM iceberg.silver.accounts a
        JOIN iceberg.silver.transactions t
            ON a.account_id = t.account_id AND t.status = 'completed'
        GROUP BY a.customer_id, t.channel
    )
    WHERE rn = 1
),
top_category AS (
    SELECT customer_id, merchant_category AS top_category
    FROM (
        SELECT
            a.customer_id,
            t.merchant_category,
            ROW_NUMBER() OVER (
                PARTITION BY a.customer_id ORDER BY COUNT(*) DESC
            ) AS rn
        FROM iceberg.silver.accounts a
        JOIN iceberg.silver.transactions t
            ON a.account_id = t.account_id AND t.status = 'completed'
        GROUP BY a.customer_id, t.merchant_category
    )
    WHERE rn = 1
)
SELECT
    c.customer_id,
    c.full_name,
    c.segment,
    c.kyc_status,
    c.city,
    COALESCE(aa.total_accounts, BIGINT '0')            AS total_accounts,
    COALESCE(aa.total_balance, CAST(0 AS DECIMAL(18,2))) AS total_balance,
    aa.primary_currency,
    COALESCE(ta.total_txn_count, BIGINT '0')           AS total_txn_count,
    COALESCE(ta.total_txn_amount, CAST(0 AS DECIMAL(18,2))) AS total_txn_amount,
    ta.last_txn_date,
    tc.preferred_channel,
    tcat.top_category,
    date_diff('day', c.join_date, CURRENT_DATE)        AS customer_since_days
FROM iceberg.silver.customers c
LEFT JOIN acct_agg aa    ON c.customer_id = aa.customer_id
LEFT JOIN txn_agg ta     ON c.customer_id = ta.customer_id
LEFT JOIN top_channel tc ON c.customer_id = tc.customer_id
LEFT JOIN top_category tcat ON c.customer_id = tcat.customer_id
```

- [ ] **Step 3: Write `pipelines/monthly_spending.yaml`**

```yaml
name: monthly_spending
description: "Per-customer monthly spending by merchant category"
schedule: "0 2 * * *"
target: gold.monthly_spending
strategy: replace
depends_on:
  - silver.accounts
  - silver.transactions
owner: data-team
tags: [spending, gold]
```

- [ ] **Step 4: Write `pipelines/monthly_spending.sql`**

```sql
SELECT
    a.customer_id,
    date_format(t.ts, '%Y-%m')             AS year_month,
    t.merchant_category,
    SUM(t.amount)                          AS total_amount,
    COUNT(t.txn_id)                        AS txn_count
FROM iceberg.silver.accounts a
JOIN iceberg.silver.transactions t
    ON a.account_id = t.account_id
    AND t.status = 'completed'
    AND t.txn_type = 'debit'
GROUP BY a.customer_id, date_format(t.ts, '%Y-%m'), t.merchant_category
```

- [ ] **Step 5: Write `pipelines/high_value_customers.yaml`**

```yaml
name: high_value_customers
description: "Customers with total balance > 50000 or avg monthly spend > 10000"
schedule: "0 2 * * *"
target: gold.high_value_customers
strategy: replace
depends_on:
  - gold.customer_profile
  - gold.monthly_spending
owner: data-team
tags: [segmentation, gold]
```

- [ ] **Step 6: Write `pipelines/high_value_customers.sql`**

```sql
WITH monthly_avg AS (
    SELECT
        customer_id,
        AVG(total_amount) AS avg_monthly_spend
    FROM iceberg.gold.monthly_spending
    GROUP BY customer_id
)
SELECT
    cp.customer_id,
    cp.full_name,
    cp.segment,
    cp.total_balance,
    COALESCE(ma.avg_monthly_spend, CAST(0 AS DECIMAL(18,2))) AS avg_monthly_spend,
    CASE
        WHEN cp.total_balance > 50000 AND COALESCE(ma.avg_monthly_spend, 0) > 10000 THEN 'both'
        WHEN cp.total_balance > 50000 THEN 'balance'
        ELSE 'spend'
    END AS qualification_reason
FROM iceberg.gold.customer_profile cp
LEFT JOIN monthly_avg ma ON cp.customer_id = ma.customer_id
WHERE cp.total_balance > 50000
   OR COALESCE(ma.avg_monthly_spend, 0) > 10000
```

- [ ] **Step 7: Commit**

```bash
git add pipelines/
git commit -m "feat: pipeline SQL+YAML definitions (customer_profile, monthly_spending, high_value_customers)"
```

---

## Task 6: Pipeline runner — tests first

**Files:**

- Create: `pipeline-runner/requirements.txt`
- Create: `pipeline-runner/tests/test_runner.py`
- Create: `pipeline-runner/run.py`
- Create: `pipeline-runner/Dockerfile`

- [ ] **Step 1: Write `pipeline-runner/requirements.txt`**

```
pyyaml==6.0.2
requests==2.32.3
pytest==8.2.0
```

- [ ] **Step 2: Write failing tests in `pipeline-runner/tests/test_runner.py`**

```python
import sys, os, textwrap, tempfile
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from run import load_pipelines, topological_sort, build_ctas_sql


def _write_pipeline(directory, name, yaml_body, sql_body):
    with open(os.path.join(directory, f"{name}.yaml"), "w") as f:
        f.write(yaml_body)
    with open(os.path.join(directory, f"{name}.sql"), "w") as f:
        f.write(sql_body)


def test_load_pipelines_reads_yaml_and_sql():
    with tempfile.TemporaryDirectory() as d:
        _write_pipeline(d, "my_table",
            "name: my_table\ntarget: gold.my_table\nstrategy: replace\ndepends_on: []\n",
            "SELECT 1 AS val")
        pipelines = load_pipelines(d)
    assert len(pipelines) == 1
    assert pipelines[0]["name"] == "my_table"
    assert pipelines[0]["sql"].strip() == "SELECT 1 AS val"
    assert pipelines[0]["target"] == "gold.my_table"


def test_load_pipelines_raises_when_sql_missing():
    import pytest
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "orphan.yaml"), "w") as f:
            f.write("name: orphan\ntarget: gold.orphan\nstrategy: replace\n")
        with pytest.raises(FileNotFoundError):
            load_pipelines(d)


def test_topological_sort_respects_gold_deps():
    pipelines = [
        {"name": "hvc", "target": "gold.hvc",
         "depends_on": ["gold.profile", "gold.spending"], "sql": ""},
        {"name": "spending", "target": "gold.spending",
         "depends_on": ["silver.transactions"], "sql": ""},
        {"name": "profile", "target": "gold.profile",
         "depends_on": ["silver.customers"], "sql": ""},
    ]
    ordered = topological_sort(pipelines)
    targets = [p["target"] for p in ordered]
    assert targets.index("gold.hvc") > targets.index("gold.profile")
    assert targets.index("gold.hvc") > targets.index("gold.spending")


def test_topological_sort_no_gold_deps_preserves_all():
    pipelines = [
        {"name": "a", "target": "gold.a", "depends_on": ["silver.x"], "sql": ""},
        {"name": "b", "target": "gold.b", "depends_on": ["silver.y"], "sql": ""},
    ]
    ordered = topological_sort(pipelines)
    assert len(ordered) == 2


def test_build_ctas_sql_replace():
    sql = build_ctas_sql("gold.customer_profile", "SELECT 1", strategy="replace")
    assert sql.startswith("CREATE OR REPLACE TABLE iceberg.gold.customer_profile AS")
    assert "SELECT 1" in sql


def test_build_ctas_sql_append():
    sql = build_ctas_sql("gold.spend", "SELECT 2", strategy="append")
    assert sql.startswith("INSERT INTO iceberg.gold.spend")
    assert "SELECT 2" in sql
```

- [ ] **Step 3: Run tests — expect failures**

```bash
cd ~/src/trino-demo/pipeline-runner
pip install -r requirements.txt -q
python -m pytest tests/test_runner.py -v 2>&1 | head -15
```

Expected: `ModuleNotFoundError: No module named 'run'`

- [ ] **Step 4: Write `pipeline-runner/run.py`**

```python
import os
import sys
import time
import glob
import yaml
import requests
from collections import defaultdict, deque

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
PIPELINES_DIR = os.getenv("PIPELINES_DIR", "/pipelines")


def load_pipelines(directory: str) -> list[dict]:
    pipelines = []
    for yaml_path in sorted(glob.glob(os.path.join(directory, "*.yaml"))):
        sql_path = yaml_path.replace(".yaml", ".sql")
        if not os.path.exists(sql_path):
            raise FileNotFoundError(f"Missing SQL for {yaml_path}")
        with open(yaml_path) as f:
            config = yaml.safe_load(f)
        with open(sql_path) as f:
            config["sql"] = f.read()
        config.setdefault("depends_on", [])
        config.setdefault("strategy", "replace")
        pipelines.append(config)
    return pipelines


def topological_sort(pipelines: list[dict]) -> list[dict]:
    gold_targets = {p["target"] for p in pipelines}
    by_target = {p["target"]: p for p in pipelines}

    # Build edges: if pipeline B depends on pipeline A's target, A → B
    successors: dict[str, set[str]] = defaultdict(set)
    in_degree: dict[str, int] = {p["target"]: 0 for p in pipelines}

    for p in pipelines:
        for dep in p.get("depends_on", []):
            if dep in gold_targets:
                successors[dep].add(p["target"])
                in_degree[p["target"]] += 1

    queue = deque(
        p for p in pipelines if in_degree[p["target"]] == 0
    )
    ordered = []
    while queue:
        node = queue.popleft()
        ordered.append(node)
        for succ_target in successors[node["target"]]:
            in_degree[succ_target] -= 1
            if in_degree[succ_target] == 0:
                queue.append(by_target[succ_target])

    return ordered if len(ordered) == len(pipelines) else pipelines


def build_ctas_sql(target: str, sql: str, strategy: str = "replace") -> str:
    full_target = f"iceberg.{target}" if not target.startswith("iceberg.") else target
    body = sql.strip()
    if strategy == "replace":
        return f"CREATE OR REPLACE TABLE {full_target} AS\n{body}"
    return f"INSERT INTO {full_target}\n{body}"


def _trino_headers() -> dict:
    return {
        "X-Trino-User": "admin",
        "Content-Type": "text/plain",
    }


def wait_for_trino(retries: int = 40, delay: int = 5) -> None:
    url = f"http://{TRINO_HOST}:{TRINO_PORT}/v1/info"
    for i in range(retries):
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200 and not r.json().get("starting", True):
                print("Trino ready")
                return
        except Exception:
            pass
        print(f"Waiting for Trino ({i + 1}/{retries})…")
        time.sleep(delay)
    raise RuntimeError("Trino did not become ready")


def run_trino_sql(sql: str) -> dict:
    url = f"http://{TRINO_HOST}:{TRINO_PORT}/v1/statement"
    r = requests.post(url, data=sql, headers=_trino_headers(), timeout=30)
    r.raise_for_status()
    result = r.json()

    while "nextUri" in result:
        time.sleep(0.3)
        r = requests.get(result["nextUri"], headers=_trino_headers(), timeout=30)
        r.raise_for_status()
        result = r.json()

    if "error" in result:
        raise RuntimeError(f"Trino error: {result['error']['message']}")
    return result


def ensure_namespace(namespace: str) -> None:
    try:
        run_trino_sql(f"CREATE SCHEMA IF NOT EXISTS iceberg.{namespace}")
    except Exception as e:
        print(f"  warn: schema creation: {e}")


def run_pipeline(pipeline: dict) -> None:
    target = pipeline["target"]
    namespace = target.split(".")[0]
    ensure_namespace(namespace)

    full_sql = build_ctas_sql(target, pipeline["sql"], pipeline.get("strategy", "replace"))
    print(f"  ▶ {pipeline['name']} → iceberg.{target}")
    start = time.time()
    run_trino_sql(full_sql)
    elapsed = time.time() - start

    count = run_trino_sql(f"SELECT COUNT(*) FROM iceberg.{target}")
    rows = count.get("data", [[0]])[0][0] if count.get("data") else "?"
    print(f"    ✓ {rows} rows in {elapsed:.1f}s")


def main() -> None:
    wait_for_trino()
    pipelines = load_pipelines(PIPELINES_DIR)
    ordered = topological_sort(pipelines)
    print(f"Running {len(ordered)} pipelines…")
    for p in ordered:
        run_pipeline(p)
    print("All pipelines complete.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run tests — expect all pass**

```bash
cd ~/src/trino-demo/pipeline-runner
python -m pytest tests/test_runner.py -v
```

Expected:

```
PASSED tests/test_runner.py::test_load_pipelines_reads_yaml_and_sql
PASSED tests/test_runner.py::test_load_pipelines_raises_when_sql_missing
PASSED tests/test_runner.py::test_topological_sort_respects_gold_deps
PASSED tests/test_runner.py::test_topological_sort_no_gold_deps_preserves_all
PASSED tests/test_runner.py::test_build_ctas_sql_replace
PASSED tests/test_runner.py::test_build_ctas_sql_append
```

- [ ] **Step 6: Write `pipeline-runner/Dockerfile`**

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY run.py .
CMD ["python", "run.py"]
```

- [ ] **Step 7: Commit**

```bash
git add pipeline-runner/
git commit -m "feat: pipeline runner with topo sort + Trino REST client + tests"
```

---

## Task 7: nginx config

**Files:**

- Create: `ui/nginx.conf`

- [ ] **Step 1: Write `ui/nginx.conf`**

```nginx
server {
    listen 3000;
    server_name _;

    root /usr/share/nginx/html;
    index index.html;

    # Proxy Trino REST API — avoids CORS issues from the browser
    location /api/trino/ {
        proxy_pass         http://trino:8080/;
        proxy_set_header   X-Trino-User admin;
        proxy_set_header   Host $host;
        proxy_read_timeout 120s;
    }

    location / {
        try_files $uri $uri/ /index.html;
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add ui/nginx.conf
git commit -m "feat: nginx config with /api/trino/ proxy"
```

---

## Task 8: Demo UI

**Files:**

- Create: `ui/index.html`

The UI reuses the exact CSS variables and component patterns from `wtg-demo.html`. Five views: Sources, Pipelines, Catalog, Query, Insights.

- [ ] **Step 1: Write `ui/index.html`**

```html
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Trino Banking Datalake</title>
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link
      href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500&family=DM+Mono:wght@400;500&display=swap"
      rel="stylesheet"
    />
    <style>
      :root {
        --bg: #0d1118;
        --bg2: #141920;
        --bg3: #1b2230;
        --bg4: #212a3a;
        --border: rgba(255, 255, 255, 0.07);
        --border2: rgba(255, 255, 255, 0.13);
        --border3: rgba(255, 255, 255, 0.2);
        --text: #dde2ee;
        --text2: #7e8ba8;
        --text3: #424f68;
        --green: #1a9268;
        --green-bg: rgba(26, 146, 104, 0.13);
        --green-text: #4ec99a;
        --blue: #2471c8;
        --blue-bg: rgba(36, 113, 200, 0.14);
        --blue-text: #6aaef5;
        --purple: #6b4ec9;
        --purple-bg: rgba(107, 78, 201, 0.14);
        --purple-text: #a992f5;
        --amber: #b87010;
        --amber-bg: rgba(184, 112, 16, 0.14);
        --amber-text: #e8b050;
        --teal-bg: rgba(16, 135, 160, 0.14);
        --teal-text: #4cc8e0;
        --red-bg: rgba(180, 50, 40, 0.14);
        --red-text: #e87060;
        --radius: 8px;
        --radius-lg: 12px;
      }
      * {
        box-sizing: border-box;
        margin: 0;
        padding: 0;
      }
      html,
      body {
        height: 100%;
        overflow: hidden;
        background: var(--bg);
        color: var(--text);
        font-family: "DM Sans", sans-serif;
        font-size: 13px;
      }
      .app {
        display: grid;
        grid-template-columns: 220px 1fr;
        height: 100vh;
      }
      .sidebar {
        background: var(--bg2);
        border-right: 1px solid var(--border);
        display: flex;
        flex-direction: column;
        overflow: hidden;
      }
      .brand {
        padding: 18px 16px 14px;
        border-bottom: 1px solid var(--border);
        display: flex;
        align-items: center;
        gap: 10px;
        flex-shrink: 0;
      }
      .brand-mark {
        width: 30px;
        height: 30px;
        border-radius: 8px;
        background: var(--blue);
        display: flex;
        align-items: center;
        justify-content: center;
        flex-shrink: 0;
      }
      .brand-mark svg {
        width: 16px;
        height: 16px;
        stroke: #fff;
        fill: none;
        stroke-width: 1.6;
        stroke-linecap: round;
        stroke-linejoin: round;
      }
      .brand-name {
        font-size: 12px;
        font-weight: 500;
        color: var(--text);
        line-height: 1.2;
      }
      .brand-sub {
        font-size: 10px;
        color: var(--text3);
        margin-top: 1px;
      }
      .nav {
        flex: 1;
        padding: 10px 8px;
        overflow-y: auto;
        display: flex;
        flex-direction: column;
        gap: 2px;
      }
      .nav-section-label {
        font-size: 10px;
        font-weight: 500;
        color: var(--text3);
        letter-spacing: 0.06em;
        text-transform: uppercase;
        padding: 10px 8px 4px;
      }
      .nav-item {
        display: flex;
        align-items: center;
        gap: 9px;
        padding: 7px 10px;
        border-radius: 7px;
        cursor: pointer;
        transition: background 0.15s;
        color: var(--text2);
        font-size: 12px;
      }
      .nav-item:hover {
        background: var(--bg3);
        color: var(--text);
      }
      .nav-item.active {
        background: var(--bg3);
        color: var(--text);
        font-weight: 500;
      }
      .nav-item svg {
        width: 15px;
        height: 15px;
        stroke: currentColor;
        fill: none;
        stroke-width: 1.5;
        stroke-linecap: round;
        stroke-linejoin: round;
        flex-shrink: 0;
        opacity: 0.7;
      }
      .nav-item.active svg {
        opacity: 1;
      }
      .nav-badge {
        margin-left: auto;
        font-size: 10px;
        padding: 1px 6px;
        border-radius: 8px;
        background: var(--bg4);
        color: var(--text3);
        font-family: "DM Mono", monospace;
      }
      .nav-item.active .nav-badge {
        background: var(--blue-bg);
        color: var(--blue-text);
      }
      .sidebar-footer {
        padding: 12px 16px;
        border-top: 1px solid var(--border);
        display: flex;
        align-items: center;
        gap: 8px;
        flex-shrink: 0;
      }
      .avatar {
        width: 26px;
        height: 26px;
        border-radius: 50%;
        background: var(--purple-bg);
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 10px;
        font-weight: 500;
        color: var(--purple-text);
        flex-shrink: 0;
      }
      .main {
        display: grid;
        grid-template-rows: 46px 1fr;
        overflow: hidden;
      }
      .topbar {
        border-bottom: 1px solid var(--border);
        display: flex;
        align-items: center;
        padding: 0 20px;
        gap: 10px;
        background: var(--bg2);
        flex-shrink: 0;
      }
      .tb-title {
        font-size: 13px;
        font-weight: 500;
        color: var(--text);
        flex: 1;
      }
      .tb-badge {
        font-size: 10px;
        padding: 2px 9px;
        border-radius: 10px;
        border: 1px solid var(--border2);
        color: var(--text2);
        font-family: "DM Mono", monospace;
      }
      .tb-btn {
        font-size: 12px;
        padding: 5px 13px;
        border-radius: var(--radius);
        border: 1px solid var(--border2);
        background: transparent;
        color: var(--text2);
        cursor: pointer;
        font-family: "DM Sans", sans-serif;
        transition: all 0.15s;
      }
      .tb-btn:hover {
        background: var(--bg3);
        color: var(--text);
      }
      .tb-btn.primary {
        background: var(--blue);
        color: #fff;
        border-color: var(--blue);
      }
      .view {
        height: 100%;
        overflow: hidden;
        display: none;
        flex-direction: column;
      }
      .view.active {
        display: flex;
      }
      /* sources */
      .sources-wrap {
        flex: 1;
        overflow-y: auto;
        padding: 20px;
      }
      .sources-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(230px, 1fr));
        gap: 10px;
      }
      .src-card {
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        padding: 14px;
        background: var(--bg2);
      }
      .src-card.silver {
        border-left: 3px solid var(--green);
      }
      .src-top {
        display: flex;
        align-items: center;
        gap: 10px;
        margin-bottom: 8px;
      }
      .src-icon {
        width: 32px;
        height: 32px;
        border-radius: 7px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 11px;
        font-weight: 500;
        flex-shrink: 0;
        font-family: "DM Mono", monospace;
      }
      .src-name {
        font-size: 12px;
        font-weight: 500;
        color: var(--text);
      }
      .src-type {
        font-size: 10px;
        color: var(--text3);
        margin-top: 2px;
      }
      .src-row {
        display: flex;
        justify-content: space-between;
        font-size: 11px;
        padding: 3px 0;
        color: var(--text2);
      }
      .src-row span:last-child {
        color: var(--text);
        font-family: "DM Mono", monospace;
      }
      /* pipeline */
      .pipeline-area {
        display: grid;
        grid-template-columns: 196px 1fr;
        flex: 1;
        overflow: hidden;
        transition: grid-template-columns 0.22s ease;
      }
      .pipeline-area.panel-open {
        grid-template-columns: 196px 1fr 280px;
      }
      .p-left {
        border-right: 1px solid var(--border);
        padding: 14px 10px;
        overflow-y: auto;
        background: var(--bg2);
        display: flex;
        flex-direction: column;
        gap: 14px;
      }
      .p-section-label {
        font-size: 10px;
        font-weight: 500;
        color: var(--text3);
        letter-spacing: 0.06em;
        text-transform: uppercase;
        margin-bottom: 5px;
      }
      .p-block {
        padding: 7px 10px;
        border: 1px solid var(--border);
        border-radius: 7px;
        font-size: 12px;
        color: var(--text2);
        display: flex;
        align-items: center;
        gap: 8px;
        background: var(--bg3);
        margin-bottom: 4px;
      }
      .p-block-dot {
        width: 7px;
        height: 7px;
        border-radius: 50%;
        flex-shrink: 0;
      }
      .p-canvas {
        background: var(--bg);
        background-image: radial-gradient(
          circle,
          rgba(255, 255, 255, 0.035) 1px,
          transparent 1px
        );
        background-size: 22px 22px;
        position: relative;
        overflow: hidden;
        flex: 1;
      }
      .pipe-node {
        position: absolute;
        border-radius: 9px;
        border: 1px solid var(--border2);
        padding: 11px 13px;
        background: var(--bg2);
        cursor: pointer;
        min-width: 160px;
        transition:
          border-color 0.15s,
          background 0.15s;
      }
      .pipe-node:hover {
        background: var(--bg3);
        border-color: var(--border3);
      }
      .pipe-node.selected {
        border-color: var(--blue) !important;
        background: var(--bg3);
        box-shadow: 0 0 0 2px rgba(36, 113, 200, 0.25);
      }
      .pipe-node.source {
        border-left: 3px solid var(--green);
      }
      .pipe-node.transform {
        border-left: 3px solid var(--blue);
      }
      .pipe-node.destination {
        border-left: 3px solid var(--amber);
      }
      .pipe-node-title {
        font-weight: 500;
        color: var(--text);
        font-size: 12px;
        margin-bottom: 3px;
      }
      .pipe-node-sub {
        font-size: 10px;
        color: var(--text2);
        line-height: 1.4;
      }
      .pipe-node-badge {
        display: inline-block;
        font-size: 10px;
        padding: 2px 7px;
        border-radius: 8px;
        margin-top: 6px;
        font-family: "DM Mono", monospace;
      }
      .canvas-svg {
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        pointer-events: none;
      }
      .canvas-footer {
        position: absolute;
        bottom: 0;
        left: 0;
        right: 0;
        border-top: 1px solid var(--border);
        background: var(--bg2);
        padding: 8px 14px;
        display: flex;
        align-items: center;
        gap: 14px;
      }
      .legend-item {
        display: flex;
        align-items: center;
        gap: 5px;
        font-size: 11px;
        color: var(--text2);
      }
      .legend-dot {
        width: 6px;
        height: 6px;
        border-radius: 50%;
        flex-shrink: 0;
      }
      .p-right {
        width: 280px;
        border-left: 1px solid var(--border);
        display: flex;
        flex-direction: column;
        overflow: hidden;
        background: var(--bg2);
      }
      .p-right-header {
        padding: 12px 14px;
        border-bottom: 1px solid var(--border);
        display: flex;
        align-items: center;
        justify-content: space-between;
      }
      .p-right-type {
        font-size: 10px;
        color: var(--text3);
        letter-spacing: 0.05em;
        text-transform: uppercase;
        margin-bottom: 2px;
      }
      .p-right-title {
        font-size: 13px;
        font-weight: 500;
        color: var(--text);
      }
      .close-btn {
        width: 22px;
        height: 22px;
        border-radius: 5px;
        border: 1px solid var(--border2);
        background: transparent;
        cursor: pointer;
        display: flex;
        align-items: center;
        justify-content: center;
        color: var(--text3);
        font-size: 16px;
        line-height: 1;
        transition: all 0.15s;
      }
      .close-btn:hover {
        background: var(--bg3);
        color: var(--text);
      }
      .p-right-body {
        padding: 12px 14px;
        overflow-y: auto;
        display: flex;
        flex-direction: column;
        gap: 10px;
        flex: 1;
      }
      .detail-card {
        border: 1px solid var(--border);
        border-radius: var(--radius);
        padding: 10px 12px;
        background: var(--bg3);
      }
      .detail-section-label {
        font-size: 10px;
        font-weight: 500;
        color: var(--text3);
        letter-spacing: 0.05em;
        text-transform: uppercase;
        margin-bottom: 8px;
      }
      .detail-row {
        display: flex;
        justify-content: space-between;
        padding: 5px 0;
        border-bottom: 1px solid var(--border);
      }
      .detail-row:last-child {
        border-bottom: none;
      }
      .detail-key {
        font-size: 11px;
        color: var(--text2);
      }
      .detail-val {
        font-size: 11px;
        font-weight: 500;
        color: var(--text);
        font-family: "DM Mono", monospace;
      }
      .detail-val.ok {
        color: var(--green-text);
      }
      .sql-block {
        font-family: "DM Mono", monospace;
        font-size: 10px;
        color: var(--text2);
        line-height: 1.7;
        padding: 10px;
        background: var(--bg);
        border-radius: 6px;
        border: 1px solid var(--border);
        white-space: pre-wrap;
        overflow-x: auto;
        max-height: 220px;
        overflow-y: auto;
      }
      /* catalog */
      .catalog-wrap {
        display: grid;
        grid-template-columns: 220px 1fr;
        flex: 1;
        overflow: hidden;
      }
      .catalog-sidebar {
        border-right: 1px solid var(--border);
        overflow-y: auto;
        background: var(--bg2);
      }
      .catalog-ns {
        padding: 10px 14px 4px;
        font-size: 10px;
        font-weight: 500;
        color: var(--text3);
        letter-spacing: 0.06em;
        text-transform: uppercase;
      }
      .catalog-table-item {
        padding: 8px 14px;
        font-size: 12px;
        color: var(--text2);
        cursor: pointer;
        display: flex;
        align-items: center;
        gap: 8px;
        transition: background 0.12s;
      }
      .catalog-table-item:hover {
        background: var(--bg3);
        color: var(--text);
      }
      .catalog-table-item.active {
        background: var(--bg3);
        color: var(--text);
        font-weight: 500;
      }
      .catalog-table-item svg {
        width: 13px;
        height: 13px;
        stroke: currentColor;
        fill: none;
        stroke-width: 1.5;
        flex-shrink: 0;
        opacity: 0.6;
      }
      .catalog-main {
        flex: 1;
        overflow-y: auto;
        padding: 20px;
        display: flex;
        flex-direction: column;
        gap: 14px;
      }
      .catalog-empty {
        display: flex;
        align-items: center;
        justify-content: center;
        height: 100%;
        color: var(--text3);
        font-size: 12px;
      }
      /* query */
      .query-wrap {
        flex: 1;
        display: grid;
        grid-template-rows: auto 1fr;
        overflow: hidden;
      }
      .query-editor-area {
        padding: 14px 20px;
        border-bottom: 1px solid var(--border);
        display: flex;
        flex-direction: column;
        gap: 8px;
        flex-shrink: 0;
      }
      textarea.sql-editor {
        width: 100%;
        height: 120px;
        background: var(--bg3);
        border: 1px solid var(--border2);
        border-radius: var(--radius);
        color: var(--text);
        font-family: "DM Mono", monospace;
        font-size: 12px;
        padding: 10px 12px;
        resize: none;
        outline: none;
        line-height: 1.6;
      }
      textarea.sql-editor:focus {
        border-color: var(--blue);
      }
      .query-results {
        flex: 1;
        overflow-y: auto;
        padding: 14px 20px;
      }
      .result-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 11px;
      }
      .result-table th {
        text-align: left;
        color: var(--text3);
        font-weight: 500;
        padding: 6px 10px;
        border-bottom: 1px solid var(--border);
        font-size: 10px;
        white-space: nowrap;
      }
      .result-table td {
        padding: 7px 10px;
        border-bottom: 1px solid var(--border);
        color: var(--text);
        font-family: "DM Mono", monospace;
        font-size: 11px;
      }
      .result-table tr:last-child td {
        border-bottom: none;
      }
      .result-meta {
        font-size: 11px;
        color: var(--text3);
        margin-bottom: 8px;
      }
      /* insights */
      .kpi-bar {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 10px;
        padding: 14px 20px;
        border-bottom: 1px solid var(--border);
        background: var(--bg2);
        flex-shrink: 0;
      }
      .kpi-card {
        background: var(--bg3);
        border-radius: var(--radius);
        padding: 12px 14px;
        border: 1px solid var(--border);
      }
      .kpi-label {
        font-size: 10px;
        color: var(--text3);
        margin-bottom: 6px;
      }
      .kpi-val {
        font-size: 22px;
        font-weight: 500;
        color: var(--text);
        line-height: 1;
        font-family: "DM Mono", monospace;
      }
      .kpi-sub {
        font-size: 10px;
        margin-top: 4px;
        color: var(--green-text);
      }
      .insights-body {
        flex: 1;
        overflow-y: auto;
        padding: 20px;
        display: flex;
        flex-direction: column;
        gap: 16px;
      }
      .pill {
        display: inline-block;
        padding: 2px 7px;
        border-radius: 8px;
        font-size: 10px;
        font-family: "DM Mono", monospace;
      }
      .pill-green {
        background: var(--green-bg);
        color: var(--green-text);
      }
      .pill-amber {
        background: var(--amber-bg);
        color: var(--amber-text);
      }
      .pill-blue {
        background: var(--blue-bg);
        color: var(--blue-text);
      }
      .pill-purple {
        background: var(--purple-bg);
        color: var(--purple-text);
      }
      ::-webkit-scrollbar {
        width: 5px;
        height: 5px;
      }
      ::-webkit-scrollbar-track {
        background: transparent;
      }
      ::-webkit-scrollbar-thumb {
        background: var(--border2);
        border-radius: 3px;
      }
    </style>
  </head>
  <body>
    <div class="app">
      <div class="sidebar">
        <div class="brand">
          <div class="brand-mark">
            <svg viewBox="0 0 16 16">
              <ellipse cx="8" cy="5.5" rx="5" ry="2.5" />
              <path d="M3 5.5v3c0 1.38 2.24 2.5 5 2.5s5-1.12 5-2.5v-3" />
              <path d="M3 8.5v3c0 1.38 2.24 2.5 5 2.5s5-1.12 5-2.5v-3" />
            </svg>
          </div>
          <div>
            <div class="brand-name">Banking Datalake</div>
            <div class="brand-sub">Trino + Iceberg + Nessie</div>
          </div>
        </div>
        <div class="nav">
          <div class="nav-section-label">Data</div>
          <div
            class="nav-item active"
            id="nav-sources"
            onclick="showView('sources')"
          >
            <svg viewBox="0 0 16 16">
              <ellipse cx="8" cy="5" rx="6" ry="2.5" />
              <path d="M2 5v3c0 1.38 2.69 2.5 6 2.5S14 9.38 14 8V5" />
              <path d="M2 8v3c0 1.38 2.69 2.5 6 2.5S14 12.38 14 11V8" />
            </svg>
            Sources
            <span class="nav-badge">3</span>
          </div>
          <div
            class="nav-item"
            id="nav-pipeline"
            onclick="showView('pipeline')"
          >
            <svg viewBox="0 0 16 16">
              <rect x="1" y="6" width="4" height="4" rx="1" />
              <rect x="6" y="2" width="4" height="4" rx="1" />
              <rect x="6" y="10" width="4" height="4" rx="1" />
              <rect x="11" y="6" width="4" height="4" rx="1" />
              <line x1="5" y1="8" x2="6" y2="8" />
              <line x1="10" y1="4" x2="11" y2="7" />
              <line x1="10" y1="12" x2="11" y2="9" />
            </svg>
            Pipelines
            <span class="nav-badge">3</span>
          </div>
          <div class="nav-item" id="nav-catalog" onclick="showView('catalog')">
            <svg viewBox="0 0 16 16">
              <path d="M2 4h12M2 8h8M2 12h5" />
              <circle cx="13" cy="10" r="2.5" />
              <path d="M15 13l1.5 1.5" />
            </svg>
            Catalog
          </div>
          <div class="nav-item" id="nav-query" onclick="showView('query')">
            <svg viewBox="0 0 16 16">
              <polyline points="3,5 6,8 3,11" />
              <line x1="8" y1="11" x2="13" y2="11" />
            </svg>
            Query
          </div>
          <div
            class="nav-item"
            id="nav-insights"
            onclick="showView('insights')"
          >
            <svg viewBox="0 0 16 16">
              <polyline points="2,12 5,8 8,10 11,5 14,7" />
              <line x1="2" y1="14" x2="14" y2="14" />
            </svg>
            Insights
          </div>
        </div>
        <div class="sidebar-footer">
          <div class="avatar">DE</div>
          <div>
            <div style="font-size:11px;color:var(--text)">Data Engineer</div>
            <div style="font-size:10px;color:var(--text3)">
              demo environment
            </div>
          </div>
        </div>
      </div>

      <div class="main">
        <div class="topbar">
          <div class="tb-title" id="tb-title">Sources</div>
          <div class="tb-badge" id="tb-badge">silver layer</div>
          <button class="tb-btn primary" id="tb-action">Refresh</button>
        </div>

        <!-- SOURCES -->
        <div class="view active" id="view-sources">
          <div class="sources-wrap">
            <div class="sources-grid">
              <div class="src-card silver">
                <div class="src-top">
                  <div
                    class="src-icon"
                    style="background:var(--green-bg);color:var(--green-text)"
                  >
                    TBL
                  </div>
                  <div>
                    <div class="src-name">silver.customers</div>
                    <div class="src-type">Iceberg · Parquet</div>
                  </div>
                </div>
                <div class="src-row">
                  <span>Rows</span><span id="src-customers-rows">—</span>
                </div>
                <div class="src-row"><span>Columns</span><span>10</span></div>
                <div class="src-row">
                  <span>Segments</span
                  ><span>retail · premium · sme · corporate</span>
                </div>
              </div>
              <div class="src-card silver">
                <div class="src-top">
                  <div
                    class="src-icon"
                    style="background:var(--green-bg);color:var(--green-text)"
                  >
                    TBL
                  </div>
                  <div>
                    <div class="src-name">silver.accounts</div>
                    <div class="src-type">Iceberg · Parquet</div>
                  </div>
                </div>
                <div class="src-row">
                  <span>Rows</span><span id="src-accounts-rows">—</span>
                </div>
                <div class="src-row"><span>Columns</span><span>8</span></div>
                <div class="src-row">
                  <span>Types</span
                  ><span>savings · checking · credit · loan</span>
                </div>
              </div>
              <div class="src-card silver">
                <div class="src-top">
                  <div
                    class="src-icon"
                    style="background:var(--green-bg);color:var(--green-text)"
                  >
                    TBL
                  </div>
                  <div>
                    <div class="src-name">silver.transactions</div>
                    <div class="src-type">Iceberg · Parquet</div>
                  </div>
                </div>
                <div class="src-row">
                  <span>Rows</span><span id="src-transactions-rows">—</span>
                </div>
                <div class="src-row"><span>Columns</span><span>10</span></div>
                <div class="src-row">
                  <span>Channels</span
                  ><span>mobile · atm · branch · online · pos</span>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- PIPELINE -->
        <div class="view" id="view-pipeline">
          <div class="pipeline-area" id="pipeline-area">
            <div class="p-left">
              <div>
                <div class="p-section-label">Silver (sources)</div>
                <div class="p-block">
                  <div
                    class="p-block-dot"
                    style="background:var(--green)"
                  ></div>
                  customers
                </div>
                <div class="p-block">
                  <div
                    class="p-block-dot"
                    style="background:var(--green)"
                  ></div>
                  accounts
                </div>
                <div class="p-block">
                  <div
                    class="p-block-dot"
                    style="background:var(--green)"
                  ></div>
                  transactions
                </div>
              </div>
              <div>
                <div class="p-section-label">Pipelines</div>
                <div class="p-block">
                  <div class="p-block-dot" style="background:var(--blue)"></div>
                  customer_profile
                </div>
                <div class="p-block">
                  <div class="p-block-dot" style="background:var(--blue)"></div>
                  monthly_spending
                </div>
                <div class="p-block">
                  <div
                    class="p-block-dot"
                    style="background:var(--amber)"
                  ></div>
                  high_value_customers
                </div>
              </div>
              <div>
                <div class="p-section-label">Gold (outputs)</div>
                <div class="p-block">
                  <div
                    class="p-block-dot"
                    style="background:var(--amber)"
                  ></div>
                  customer_profile
                </div>
                <div class="p-block">
                  <div
                    class="p-block-dot"
                    style="background:var(--amber)"
                  ></div>
                  monthly_spending
                </div>
                <div class="p-block">
                  <div
                    class="p-block-dot"
                    style="background:var(--amber)"
                  ></div>
                  high_value_customers
                </div>
              </div>
            </div>
            <div
              class="p-canvas"
              id="canvas"
              onclick="handleCanvasClick(event)"
            >
              <svg class="canvas-svg" id="canvas-svg">
                <defs>
                  <marker
                    id="arr"
                    viewBox="0 0 10 10"
                    refX="8"
                    refY="5"
                    markerWidth="5"
                    markerHeight="5"
                    orient="auto-start-reverse"
                  >
                    <path
                      d="M2 2L8 5L2 8"
                      fill="none"
                      stroke="rgba(255,255,255,0.18)"
                      stroke-width="1.5"
                      stroke-linecap="round"
                    />
                  </marker>
                  <marker
                    id="arr-blue"
                    viewBox="0 0 10 10"
                    refX="8"
                    refY="5"
                    markerWidth="5"
                    markerHeight="5"
                    orient="auto-start-reverse"
                  >
                    <path
                      d="M2 2L8 5L2 8"
                      fill="none"
                      stroke="rgba(36,113,200,0.55)"
                      stroke-width="1.5"
                      stroke-linecap="round"
                    />
                  </marker>
                  <marker
                    id="arr-amber"
                    viewBox="0 0 10 10"
                    refX="8"
                    refY="5"
                    markerWidth="5"
                    markerHeight="5"
                    orient="auto-start-reverse"
                  >
                    <path
                      d="M2 2L8 5L2 8"
                      fill="none"
                      stroke="rgba(184,112,16,0.55)"
                      stroke-width="1.5"
                      stroke-linecap="round"
                    />
                  </marker>
                </defs>
              </svg>
              <!-- Silver sources -->
              <div
                class="pipe-node source"
                id="node-customers"
                style="left:24px;top:50px"
                onclick="selectNode('customers',event)"
              >
                <div class="pipe-node-title">silver.customers</div>
                <div class="pipe-node-sub">1,000 rows · 10 cols</div>
                <span
                  class="pipe-node-badge"
                  style="background:var(--green-bg);color:var(--green-text)"
                  >silver</span
                >
              </div>
              <div
                class="pipe-node source"
                id="node-accounts"
                style="left:24px;top:160px"
                onclick="selectNode('accounts',event)"
              >
                <div class="pipe-node-title">silver.accounts</div>
                <div class="pipe-node-sub">1,500 rows · 8 cols</div>
                <span
                  class="pipe-node-badge"
                  style="background:var(--green-bg);color:var(--green-text)"
                  >silver</span
                >
              </div>
              <div
                class="pipe-node source"
                id="node-transactions"
                style="left:24px;top:270px"
                onclick="selectNode('transactions',event)"
              >
                <div class="pipe-node-title">silver.transactions</div>
                <div class="pipe-node-sub">50,000 rows · 10 cols</div>
                <span
                  class="pipe-node-badge"
                  style="background:var(--green-bg);color:var(--green-text)"
                  >silver</span
                >
              </div>
              <!-- Pipeline transforms -->
              <div
                class="pipe-node transform"
                id="node-cp-pipe"
                style="left:260px;top:80px"
                onclick="selectNode('cp-pipe',event)"
              >
                <div class="pipe-node-title">customer_profile</div>
                <div class="pipe-node-sub">Customer360 · CTAS replace</div>
                <span
                  class="pipe-node-badge"
                  style="background:var(--blue-bg);color:var(--blue-text)"
                  >pipeline</span
                >
              </div>
              <div
                class="pipe-node transform"
                id="node-ms-pipe"
                style="left:260px;top:210px"
                onclick="selectNode('ms-pipe',event)"
              >
                <div class="pipe-node-title">monthly_spending</div>
                <div class="pipe-node-sub">
                  Spend by category/month · CTAS replace
                </div>
                <span
                  class="pipe-node-badge"
                  style="background:var(--blue-bg);color:var(--blue-text)"
                  >pipeline</span
                >
              </div>
              <!-- Gold outputs -->
              <div
                class="pipe-node destination"
                id="node-cp-gold"
                style="left:510px;top:50px"
                onclick="selectNode('cp-gold',event)"
              >
                <div class="pipe-node-title">gold.customer_profile</div>
                <div class="pipe-node-sub">Customer360 view</div>
                <span
                  class="pipe-node-badge"
                  style="background:var(--amber-bg);color:var(--amber-text)"
                  >gold</span
                >
              </div>
              <div
                class="pipe-node destination"
                id="node-ms-gold"
                style="left:510px;top:160px"
                onclick="selectNode('ms-gold',event)"
              >
                <div class="pipe-node-title">gold.monthly_spending</div>
                <div class="pipe-node-sub">Spending breakdown</div>
                <span
                  class="pipe-node-badge"
                  style="background:var(--amber-bg);color:var(--amber-text)"
                  >gold</span
                >
              </div>
              <div
                class="pipe-node destination"
                id="node-hvc-gold"
                style="left:510px;top:280px"
                onclick="selectNode('hvc-gold',event)"
              >
                <div class="pipe-node-title">gold.high_value_customers</div>
                <div class="pipe-node-sub">Tier-1 segmentation</div>
                <span
                  class="pipe-node-badge"
                  style="background:var(--amber-bg);color:var(--amber-text)"
                  >gold</span
                >
              </div>
              <!-- HVC pipeline (depends on gold.customer_profile + gold.monthly_spending) -->
              <div
                class="pipe-node transform"
                id="node-hvc-pipe"
                style="left:750px;top:210px"
                onclick="selectNode('hvc-pipe',event)"
              >
                <div class="pipe-node-title">high_value_customers</div>
                <div class="pipe-node-sub">From gold · CTAS replace</div>
                <span
                  class="pipe-node-badge"
                  style="background:var(--amber-bg);color:var(--amber-text)"
                  >pipeline</span
                >
              </div>
              <div class="canvas-footer">
                <div class="legend-item">
                  <div class="legend-dot" style="background:var(--green)"></div>
                  Silver
                </div>
                <div class="legend-item">
                  <div class="legend-dot" style="background:var(--blue)"></div>
                  Pipeline
                </div>
                <div class="legend-item">
                  <div class="legend-dot" style="background:var(--amber)"></div>
                  Gold
                </div>
                <span style="margin-left:auto;font-size:11px;color:var(--text3)"
                  >Click any node to inspect</span
                >
              </div>
            </div>
            <div class="p-right" id="p-right" style="display:none">
              <div class="p-right-header">
                <div>
                  <div class="p-right-type" id="panel-type"></div>
                  <div class="p-right-title" id="panel-title"></div>
                </div>
                <button class="close-btn" onclick="closePanel()">×</button>
              </div>
              <div class="p-right-body" id="panel-body"></div>
            </div>
          </div>
        </div>

        <!-- CATALOG -->
        <div class="view" id="view-catalog">
          <div class="catalog-wrap">
            <div class="catalog-sidebar">
              <div class="catalog-ns">silver</div>
              <div
                class="catalog-table-item"
                onclick="loadTable('silver','customers')"
              >
                <svg viewBox="0 0 13 13">
                  <rect x="1" y="2" width="11" height="9" rx="1" />
                  <line x1="1" y1="5" x2="12" y2="5" />
                  <line x1="4" y1="2" x2="4" y2="11" />
                </svg>
                customers
              </div>
              <div
                class="catalog-table-item"
                onclick="loadTable('silver','accounts')"
              >
                <svg viewBox="0 0 13 13">
                  <rect x="1" y="2" width="11" height="9" rx="1" />
                  <line x1="1" y1="5" x2="12" y2="5" />
                  <line x1="4" y1="2" x2="4" y2="11" />
                </svg>
                accounts
              </div>
              <div
                class="catalog-table-item"
                onclick="loadTable('silver','transactions')"
              >
                <svg viewBox="0 0 13 13">
                  <rect x="1" y="2" width="11" height="9" rx="1" />
                  <line x1="1" y1="5" x2="12" y2="5" />
                  <line x1="4" y1="2" x2="4" y2="11" />
                </svg>
                transactions
              </div>
              <div class="catalog-ns" style="margin-top:8px">gold</div>
              <div
                class="catalog-table-item"
                onclick="loadTable('gold','customer_profile')"
              >
                <svg viewBox="0 0 13 13">
                  <rect x="1" y="2" width="11" height="9" rx="1" />
                  <line x1="1" y1="5" x2="12" y2="5" />
                  <line x1="4" y1="2" x2="4" y2="11" />
                </svg>
                customer_profile
              </div>
              <div
                class="catalog-table-item"
                onclick="loadTable('gold','monthly_spending')"
              >
                <svg viewBox="0 0 13 13">
                  <rect x="1" y="2" width="11" height="9" rx="1" />
                  <line x1="1" y1="5" x2="12" y2="5" />
                  <line x1="4" y1="2" x2="4" y2="11" />
                </svg>
                monthly_spending
              </div>
              <div
                class="catalog-table-item"
                onclick="loadTable('gold','high_value_customers')"
              >
                <svg viewBox="0 0 13 13">
                  <rect x="1" y="2" width="11" height="9" rx="1" />
                  <line x1="1" y1="5" x2="12" y2="5" />
                  <line x1="4" y1="2" x2="4" y2="11" />
                </svg>
                high_value_customers
              </div>
            </div>
            <div class="catalog-main" id="catalog-main">
              <div class="catalog-empty">
                Select a table to browse schema and sample data
              </div>
            </div>
          </div>
        </div>

        <!-- QUERY -->
        <div class="view" id="view-query">
          <div class="query-wrap">
            <div class="query-editor-area">
              <textarea
                class="sql-editor"
                id="sql-input"
                placeholder="SELECT * FROM iceberg.gold.customer_profile LIMIT 10"
              >
SELECT * FROM iceberg.gold.customer_profile LIMIT 10</textarea
              >
              <div style="display:flex;gap:8px;align-items:center">
                <button class="tb-btn primary" onclick="runQuery()">
                  ▶ Run
                </button>
                <button
                  class="tb-btn"
                  onclick="document.getElementById('sql-input').value=''"
                >
                  Clear
                </button>
                <span
                  id="query-status"
                  style="font-size:11px;color:var(--text3);margin-left:4px"
                ></span>
              </div>
            </div>
            <div class="query-results" id="query-results">
              <div style="color:var(--text3);font-size:12px;padding:20px 0">
                Run a query to see results
              </div>
            </div>
          </div>
        </div>

        <!-- INSIGHTS -->
        <div class="view" id="view-insights">
          <div class="kpi-bar">
            <div class="kpi-card">
              <div class="kpi-label">Total customers</div>
              <div class="kpi-val" id="kpi-customers">—</div>
              <div class="kpi-sub">silver.customers</div>
            </div>
            <div class="kpi-card">
              <div class="kpi-label">High-value customers</div>
              <div class="kpi-val" id="kpi-hvc">—</div>
              <div class="kpi-sub">gold.high_value_customers</div>
            </div>
            <div class="kpi-card">
              <div class="kpi-label">Total transactions</div>
              <div class="kpi-val" id="kpi-txns">—</div>
              <div class="kpi-sub">silver.transactions</div>
            </div>
            <div class="kpi-card">
              <div class="kpi-label">Total transaction volume</div>
              <div class="kpi-val" id="kpi-volume">—</div>
              <div class="kpi-sub">sum(amount)</div>
            </div>
          </div>
          <div class="insights-body">
            <div
              style="border:1px solid var(--border);border-radius:var(--radius-lg);background:var(--bg2);overflow:hidden"
            >
              <div
                style="padding:10px 14px;border-bottom:1px solid var(--border);font-size:12px;font-weight:500;color:var(--text)"
              >
                Top spending categories
              </div>
              <div id="insights-categories" style="padding:12px 14px"></div>
            </div>
            <div
              style="border:1px solid var(--border);border-radius:var(--radius-lg);background:var(--bg2);overflow:hidden"
            >
              <div
                style="padding:10px 14px;border-bottom:1px solid var(--border);font-size:12px;font-weight:500;color:var(--text)"
              >
                High-value customers — top 10 by balance
              </div>
              <div id="insights-hvc" style="padding:0"></div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <script>
      const TRINO = "/api/trino";

      // ── Trino REST client ──────────────────────────────────────────────────────────
      async function trinoQuery(sql) {
        const r = await fetch(`${TRINO}/v1/statement`, {
          method: "POST",
          headers: { "Content-Type": "text/plain", "X-Trino-User": "admin" },
          body: sql,
        });
        let result = await r.json();
        while (result.nextUri) {
          await new Promise((res) => setTimeout(res, 300));
          const nr = await fetch(result.nextUri, {
            headers: { "X-Trino-User": "admin" },
          });
          result = await nr.json();
        }
        if (result.error) throw new Error(result.error.message);
        return result;
      }

      async function trinoScalar(sql) {
        const r = await trinoQuery(sql);
        return r.data ? r.data[0][0] : null;
      }

      // ── Navigation ────────────────────────────────────────────────────────────────
      const viewCfg = {
        sources: { title: "Sources", badge: "silver layer", action: "Refresh" },
        pipeline: {
          title: "Pipelines",
          badge: "3 pipelines",
          action: "Run all",
        },
        catalog: {
          title: "Data Catalog",
          badge: "6 tables",
          action: "Refresh",
        },
        query: {
          title: "SQL Query",
          badge: "Trino · iceberg",
          action: "Clear",
        },
        insights: { title: "Insights", badge: "gold layer", action: "Refresh" },
      };

      function showView(v) {
        document
          .querySelectorAll(".view")
          .forEach((el) => el.classList.remove("active"));
        document.getElementById("view-" + v).classList.add("active");
        ["sources", "pipeline", "catalog", "query", "insights"].forEach((n) => {
          document
            .getElementById("nav-" + n)
            .classList.toggle("active", n === v);
        });
        const cfg = viewCfg[v];
        document.getElementById("tb-title").textContent = cfg.title;
        document.getElementById("tb-badge").textContent = cfg.badge;
        document.getElementById("tb-action").textContent = cfg.action;
        document.getElementById("tb-action").onclick = () => onViewAction(v);
        if (v === "pipeline") setTimeout(drawEdges, 50);
        if (v === "insights") loadInsights();
        if (v === "sources") loadSourceCounts();
      }

      function onViewAction(v) {
        if (v === "sources" || v === "insights") location.reload();
        if (v === "query") {
          document.getElementById("sql-input").value = "";
          document.getElementById("query-results").innerHTML = "";
        }
        if (v === "pipeline") setTimeout(drawEdges, 50);
      }

      // ── Sources ───────────────────────────────────────────────────────────────────
      async function loadSourceCounts() {
        for (const [ns, tbl, el] of [
          ["silver", "customers", "src-customers-rows"],
          ["silver", "accounts", "src-accounts-rows"],
          ["silver", "transactions", "src-transactions-rows"],
        ]) {
          try {
            const n = await trinoScalar(
              `SELECT COUNT(*) FROM iceberg.${ns}.${tbl}`,
            );
            document.getElementById(el).textContent =
              Number(n).toLocaleString();
          } catch (e) {
            document.getElementById(el).textContent = "error";
          }
        }
      }

      // ── Pipeline canvas ───────────────────────────────────────────────────────────
      const nodeData = {
        customers: {
          type: "Silver",
          title: "silver.customers",
          rows: [
            { k: "Rows", v: "1,000" },
            { k: "Columns", v: "10" },
            { k: "Format", v: "Iceberg/Parquet" },
            { k: "Partition", v: "none" },
            { k: "Status", v: "seeded", c: "ok" },
          ],
        },
        accounts: {
          type: "Silver",
          title: "silver.accounts",
          rows: [
            { k: "Rows", v: "1,500" },
            { k: "Columns", v: "8" },
            { k: "Format", v: "Iceberg/Parquet" },
            { k: "Status", v: "seeded", c: "ok" },
          ],
        },
        transactions: {
          type: "Silver",
          title: "silver.transactions",
          rows: [
            { k: "Rows", v: "50,000" },
            { k: "Columns", v: "10" },
            { k: "Format", v: "Iceberg/Parquet" },
            { k: "Status", v: "seeded", c: "ok" },
          ],
        },
        "cp-pipe": {
          type: "Pipeline",
          title: "customer_profile",
          yaml: 'name: customer_profile\ntarget: gold.customer_profile\nstrategy: replace\nschedule: "0 2 * * *"\ndepends_on:\n  - silver.customers\n  - silver.accounts\n  - silver.transactions\nowner: data-team',
          sql: "WITH acct_agg AS (\n  SELECT customer_id,\n    COUNT(account_id) AS total_accounts,\n    SUM(balance) AS total_balance\n  FROM iceberg.silver.accounts\n  WHERE status = 'active'\n  GROUP BY customer_id\n)\nSELECT c.customer_id, c.full_name, ...\nFROM iceberg.silver.customers c\nLEFT JOIN acct_agg aa ON c.customer_id = aa.customer_id",
        },
        "ms-pipe": {
          type: "Pipeline",
          title: "monthly_spending",
          yaml: 'name: monthly_spending\ntarget: gold.monthly_spending\nstrategy: replace\nschedule: "0 2 * * *"\ndepends_on:\n  - silver.accounts\n  - silver.transactions\nowner: data-team',
          sql: "SELECT a.customer_id,\n  date_format(t.ts, '%Y-%m') AS year_month,\n  t.merchant_category,\n  SUM(t.amount) AS total_amount,\n  COUNT(t.txn_id) AS txn_count\nFROM iceberg.silver.accounts a\nJOIN iceberg.silver.transactions t\n  ON a.account_id = t.account_id\nGROUP BY 1, 2, 3",
        },
        "hvc-pipe": {
          type: "Pipeline",
          title: "high_value_customers",
          yaml: 'name: high_value_customers\ntarget: gold.high_value_customers\nstrategy: replace\nschedule: "0 2 * * *"\ndepends_on:\n  - gold.customer_profile\n  - gold.monthly_spending\nowner: data-team',
          sql: "WITH monthly_avg AS (\n  SELECT customer_id, AVG(total_amount) AS avg_monthly_spend\n  FROM iceberg.gold.monthly_spending\n  GROUP BY customer_id\n)\nSELECT cp.customer_id, cp.full_name, ...\nFROM iceberg.gold.customer_profile cp\nWHERE cp.total_balance > 50000\n   OR avg_monthly_spend > 10000",
        },
        "cp-gold": {
          type: "Gold",
          title: "gold.customer_profile",
          rows: [
            { k: "Rows", v: "~1,000" },
            { k: "Strategy", v: "replace" },
            { k: "Schedule", v: "02:00 daily" },
            { k: "Status", v: "ready", c: "ok" },
          ],
        },
        "ms-gold": {
          type: "Gold",
          title: "gold.monthly_spending",
          rows: [
            { k: "Rows", v: "~varies" },
            { k: "Strategy", v: "replace" },
            { k: "Schedule", v: "02:00 daily" },
            { k: "Status", v: "ready", c: "ok" },
          ],
        },
        "hvc-gold": {
          type: "Gold",
          title: "gold.high_value_customers",
          rows: [
            { k: "Rows", v: "~varies" },
            { k: "Strategy", v: "replace" },
            { k: "Schedule", v: "02:00 daily" },
            { k: "Status", v: "ready", c: "ok" },
          ],
        },
      };

      let selectedNode = null;

      function drawEdges() {
        const canvas = document.getElementById("canvas");
        const svg = document.getElementById("canvas-svg");
        svg.setAttribute("width", canvas.offsetWidth);
        svg.setAttribute("height", canvas.offsetHeight);
        svg.querySelectorAll("path.edge").forEach((e) => e.remove());

        const edges = [
          { f: "node-customers", t: "node-cp-pipe", marker: "arr-blue" },
          { f: "node-accounts", t: "node-cp-pipe", marker: "arr-blue" },
          { f: "node-accounts", t: "node-ms-pipe", marker: "arr-blue" },
          { f: "node-transactions", t: "node-ms-pipe", marker: "arr-blue" },
          { f: "node-cp-pipe", t: "node-cp-gold", marker: "arr-blue" },
          { f: "node-ms-pipe", t: "node-ms-gold", marker: "arr-blue" },
          { f: "node-cp-gold", t: "node-hvc-pipe", marker: "arr-amber" },
          { f: "node-ms-gold", t: "node-hvc-pipe", marker: "arr-amber" },
          { f: "node-hvc-pipe", t: "node-hvc-gold", marker: "arr-amber" },
        ];

        edges.forEach(({ f, t, marker }) => {
          const a = document.getElementById(f),
            b = document.getElementById(t);
          if (!a || !b) return;
          const ax = a.offsetLeft + a.offsetWidth,
            ay = a.offsetTop + a.offsetHeight / 2;
          const bx = b.offsetLeft,
            by = b.offsetTop + b.offsetHeight / 2;
          const mx = (ax + bx) / 2;
          const p = document.createElementNS(
            "http://www.w3.org/2000/svg",
            "path",
          );
          p.setAttribute("class", "edge");
          p.setAttribute(
            "d",
            `M${ax},${ay} C${mx},${ay} ${mx},${by} ${bx},${by}`,
          );
          p.setAttribute("fill", "none");
          p.setAttribute(
            "stroke",
            marker === "arr-amber"
              ? "rgba(184,112,16,0.45)"
              : "rgba(36,113,200,0.45)",
          );
          p.setAttribute("stroke-width", "1");
          p.setAttribute("marker-end", `url(#${marker})`);
          svg.appendChild(p);
        });
      }

      function selectNode(id, e) {
        e.stopPropagation();
        if (selectedNode === id) {
          closePanel();
          return;
        }
        selectedNode = id;
        document
          .querySelectorAll(".pipe-node")
          .forEach((n) => n.classList.remove("selected"));
        document.getElementById("node-" + id).classList.add("selected");
        const d = nodeData[id];
        document.getElementById("panel-type").textContent = d.type;
        document.getElementById("panel-title").textContent = d.title;

        let body = "";
        if (d.rows) {
          const rows = d.rows
            .map(
              (r) =>
                `<div class="detail-row"><span class="detail-key">${r.k}</span><span class="detail-val ${r.c || ""}">${r.v}</span></div>`,
            )
            .join("");
          body += `<div class="detail-card"><div class="detail-section-label">Properties</div>${rows}</div>`;
        }
        if (d.yaml) {
          body += `<div class="detail-card"><div class="detail-section-label">Pipeline YAML</div><div class="sql-block">${d.yaml}</div></div>`;
        }
        if (d.sql) {
          body += `<div class="detail-card"><div class="detail-section-label">SQL (preview)</div><div class="sql-block">${d.sql}</div></div>`;
        }
        document.getElementById("panel-body").innerHTML = body;
        document.getElementById("p-right").style.display = "flex";
        document.getElementById("p-right").style.flexDirection = "column";
        document.getElementById("pipeline-area").classList.add("panel-open");
        setTimeout(drawEdges, 250);
      }

      function closePanel() {
        selectedNode = null;
        document
          .querySelectorAll(".pipe-node")
          .forEach((n) => n.classList.remove("selected"));
        document.getElementById("p-right").style.display = "none";
        document.getElementById("pipeline-area").classList.remove("panel-open");
        setTimeout(drawEdges, 250);
      }

      function handleCanvasClick(e) {
        if (!e.target.closest(".pipe-node")) closePanel();
      }

      // ── Catalog ───────────────────────────────────────────────────────────────────
      async function loadTable(ns, tbl) {
        document
          .querySelectorAll(".catalog-table-item")
          .forEach((el) => el.classList.remove("active"));
        event.currentTarget.classList.add("active");
        const main = document.getElementById("catalog-main");
        main.innerHTML =
          '<div style="color:var(--text3);font-size:12px;padding:10px 0">Loading…</div>';
        try {
          const [desc, sample] = await Promise.all([
            trinoQuery(`DESCRIBE iceberg.${ns}.${tbl}`),
            trinoQuery(`SELECT * FROM iceberg.${ns}.${tbl} LIMIT 10`),
          ]);

          const cols = (desc.data || [])
            .map(
              (r) =>
                `<tr><td style="color:var(--text)">${r[0]}</td><td style="font-family:'DM Mono',monospace;color:var(--text3)">${r[1]}</td></tr>`,
            )
            .join("");

          const sampleCols = (sample.columns || []).map((c) => c.name);
          const sampleRows = (sample.data || [])
            .map(
              (r) =>
                `<tr>${r.map((v) => `<td>${v ?? '<span style="color:var(--text3)">null</span>'}</td>`).join("")}</tr>`,
            )
            .join("");
          const sampleHdr = sampleCols.map((c) => `<th>${c}</th>`).join("");

          main.innerHTML = `
      <div style="font-size:13px;font-weight:500;color:var(--text)">iceberg.${ns}.${tbl}</div>
      <div style="border:1px solid var(--border);border-radius:var(--radius-lg);background:var(--bg2);overflow:hidden">
        <div style="padding:10px 14px;border-bottom:1px solid var(--border);font-size:11px;font-weight:500;color:var(--text)">Schema</div>
        <table class="result-table" style="margin:0"><thead><tr><th>Column</th><th>Type</th></tr></thead><tbody>${cols}</tbody></table>
      </div>
      <div style="border:1px solid var(--border);border-radius:var(--radius-lg);background:var(--bg2);overflow:hidden">
        <div style="padding:10px 14px;border-bottom:1px solid var(--border);font-size:11px;font-weight:500;color:var(--text)">Sample data (10 rows)</div>
        <div style="overflow-x:auto"><table class="result-table" style="margin:0"><thead><tr>${sampleHdr}</tr></thead><tbody>${sampleRows}</tbody></table></div>
      </div>`;
        } catch (e) {
          main.innerHTML = `<div style="color:var(--red-text);font-size:12px">${e.message}</div>`;
        }
      }

      // ── Query runner ──────────────────────────────────────────────────────────────
      async function runQuery() {
        const sql = document.getElementById("sql-input").value.trim();
        if (!sql) return;
        const status = document.getElementById("query-status");
        const results = document.getElementById("query-results");
        status.textContent = "Running…";
        results.innerHTML = "";
        const t0 = Date.now();
        try {
          const r = await trinoQuery(sql);
          const elapsed = ((Date.now() - t0) / 1000).toFixed(2);
          const cols = (r.columns || []).map((c) => c.name);
          const rows = r.data || [];
          const hdr = cols.map((c) => `<th>${c}</th>`).join("");
          const body = rows
            .map(
              (r) => `<tr>${r.map((v) => `<td>${v ?? ""}</td>`).join("")}</tr>`,
            )
            .join("");
          results.innerHTML = `
      <div class="result-meta">${rows.length} row${rows.length !== 1 ? "s" : ""} · ${elapsed}s</div>
      <div style="overflow-x:auto"><table class="result-table"><thead><tr>${hdr}</tr></thead><tbody>${body}</tbody></table></div>`;
          status.textContent = `✓ ${elapsed}s`;
        } catch (e) {
          results.innerHTML = `<div style="color:var(--red-text);font-size:12px;padding:10px 0">${e.message}</div>`;
          status.textContent = "error";
        }
      }

      document.getElementById("sql-input").addEventListener("keydown", (e) => {
        if ((e.metaKey || e.ctrlKey) && e.key === "Enter") runQuery();
      });

      // ── Insights ─────────────────────────────────────────────────────────────────
      async function loadInsights() {
        // KPI cards
        const kpis = [
          ["kpi-customers", "SELECT COUNT(*) FROM iceberg.silver.customers"],
          ["kpi-hvc", "SELECT COUNT(*) FROM iceberg.gold.high_value_customers"],
          ["kpi-txns", "SELECT COUNT(*) FROM iceberg.silver.transactions"],
          [
            "kpi-volume",
            "SELECT CAST(SUM(amount) AS BIGINT) FROM iceberg.silver.transactions WHERE status='completed'",
          ],
        ];
        for (const [id, sql] of kpis) {
          try {
            const v = await trinoScalar(sql);
            document.getElementById(id).textContent =
              Number(v).toLocaleString();
          } catch {
            document.getElementById(id).textContent = "n/a";
          }
        }

        // Top categories
        try {
          const r = await trinoQuery(`
      SELECT merchant_category, SUM(total_amount) AS total
      FROM iceberg.gold.monthly_spending
      GROUP BY merchant_category
      ORDER BY total DESC
      LIMIT 8`);
          const rows = r.data || [];
          const max = rows.length ? Number(rows[0][1]) : 1;
          const colors = [
            "var(--blue)",
            "var(--green)",
            "var(--purple)",
            "var(--amber)",
            "var(--teal-text)",
          ];
          document.getElementById("insights-categories").innerHTML = rows
            .map((row, i) => {
              const pct = Math.round((Number(row[1]) / max) * 100);
              return `<div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
        <div style="width:110px;font-size:11px;color:var(--text2);text-align:right">${row[0]}</div>
        <div style="flex:1;height:18px;background:var(--bg3);border-radius:4px;overflow:hidden;border:1px solid var(--border)">
          <div style="height:100%;width:${pct}%;background:${colors[i % colors.length]};border-radius:3px"></div>
        </div>
        <div style="font-size:11px;color:var(--text2);width:80px;font-family:'DM Mono',monospace">$${Math.round(Number(row[1]) / 1000)}K</div>
      </div>`;
            })
            .join("");
        } catch {
          document.getElementById("insights-categories").textContent =
            "Run pipeline runner first";
        }

        // High-value customers table
        try {
          const r = await trinoQuery(`
      SELECT full_name, segment, total_balance, qualification_reason
      FROM iceberg.gold.high_value_customers
      ORDER BY total_balance DESC
      LIMIT 10`);
          const rows = r.data || [];
          const segColor = {
            premium: "var(--blue-bg)",
            retail: "var(--green-bg)",
            sme: "var(--teal-bg)",
            corporate: "var(--purple-bg)",
          };
          const segText = {
            premium: "var(--blue-text)",
            retail: "var(--green-text)",
            sme: "var(--teal-text)",
            corporate: "var(--purple-text)",
          };
          document.getElementById("insights-hvc").innerHTML =
            `<table class="result-table">
      <thead><tr><th>Name</th><th>Segment</th><th>Total balance</th><th>Reason</th></tr></thead>
      <tbody>${rows
        .map(
          (r) => `<tr>
        <td>${r[0]}</td>
        <td><span class="pill" style="background:${segColor[r[1]] || "var(--bg4)"};color:${segText[r[1]] || "var(--text3)"}">${r[1]}</span></td>
        <td style="font-family:'DM Mono',monospace">$${Number(r[2]).toLocaleString()}</td>
        <td><span class="pill pill-amber">${r[3]}</span></td>
      </tr>`,
        )
        .join("")}</tbody></table>`;
        } catch {
          document.getElementById("insights-hvc").innerHTML =
            '<div style="padding:12px 14px;font-size:12px;color:var(--text3)">Run pipeline runner first</div>';
        }
      }

      // ── Init ──────────────────────────────────────────────────────────────────────
      window.addEventListener("resize", () => {
        if (
          document.getElementById("view-pipeline").classList.contains("active")
        )
          drawEdges();
      });
      loadSourceCounts();
      setTimeout(drawEdges, 200);
    </script>
  </body>
</html>
```

- [ ] **Step 2: Commit**

```bash
git add ui/index.html
git commit -m "feat: dark-themed catalog UI (sources, pipelines, catalog, query, insights)"
```

---

## Task 9: Helm chart

**Files:**

- Create: `helm/trino-demo/Chart.yaml`
- Create: `helm/trino-demo/values.yaml`
- Create: `helm/trino-demo/templates/nessie.yaml`
- Create: `helm/trino-demo/templates/seed-job.yaml`
- Create: `helm/trino-demo/templates/pipeline-runner.yaml`
- Create: `helm/trino-demo/templates/ui.yaml`

- [ ] **Step 1: Write `helm/trino-demo/Chart.yaml`**

```yaml
apiVersion: v2
name: trino-demo
description: Banking datalake demo — Trino + Nessie + MinIO
version: 0.1.0
appVersion: "1.0.0"
dependencies:
  - name: trino
    version: "0.35.0"
    repository: https://trinodb.github.io/charts
  - name: minio
    version: "14.6.16"
    repository: https://charts.bitnami.com/bitnami
```

- [ ] **Step 2: Write `helm/trino-demo/values.yaml`**

```yaml
trino:
  coordinator:
    resources:
      requests:
        memory: 2Gi
        cpu: "1"
  additionalConfigFiles:
    iceberg.properties: |
      connector.name=iceberg
      iceberg.catalog.type=rest
      iceberg.rest-catalog.uri=http://{{ .Release.Name }}-nessie:19120/iceberg
      iceberg.rest-catalog.warehouse=s3://datalake/
      fs.native-s3.enabled=true
      s3.endpoint=http://{{ .Release.Name }}-minio:9000
      s3.path-style-access=true
      s3.aws-access-key=minioadmin
      s3.aws-secret-key=minioadmin
      s3.region=us-east-1

minio:
  auth:
    rootUser: minioadmin
    rootPassword: minioadmin
  defaultBuckets: "datalake"
  persistence:
    enabled: false

nessie:
  image: projectnessie/nessie:0.80.0
  storage: IN_MEMORY

seed:
  image: "" # set to your built seed image: registry/trino-demo-seed:latest
  numCustomers: "1000"
  numAccounts: "1500"
  numTransactions: "50000"

pipelineRunner:
  image: "" # set to your built pipeline-runner image
  schedule: "0 2 * * *"

ui:
  image: nginx:1.27-alpine
  port: 3000
```

- [ ] **Step 3: Write `helm/trino-demo/templates/nessie.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Release.Name }}-nessie
spec:
  replicas: 1
  selector:
    matchLabels:
      app: {{ .Release.Name }}-nessie
  template:
    metadata:
      labels:
        app: {{ .Release.Name }}-nessie
    spec:
      containers:
        - name: nessie
          image: {{ .Values.nessie.image }}
          env:
            - name: QUARKUS_PROFILE
              value: prod
            - name: NESSIE_VERSION_STORE_TYPE
              value: {{ .Values.nessie.storage }}
          ports:
            - containerPort: 19120
          readinessProbe:
            httpGet:
              path: /api/v2/config
              port: 19120
            initialDelaySeconds: 10
            periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: {{ .Release.Name }}-nessie
spec:
  selector:
    app: {{ .Release.Name }}-nessie
  ports:
    - port: 19120
      targetPort: 19120
```

- [ ] **Step 4: Write `helm/trino-demo/templates/seed-job.yaml`**

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ .Release.Name }}-seed
  annotations:
    "helm.sh/hook": post-install
    "helm.sh/hook-weight": "1"
    "helm.sh/hook-delete-policy": hook-succeeded
spec:
  backoffLimit: 3
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: seed
          image: {{ .Values.seed.image }}
          env:
            - name: MINIO_ENDPOINT
              value: http://{{ .Release.Name }}-minio:9000
            - name: NESSIE_URI
              value: http://{{ .Release.Name }}-nessie:19120/iceberg
            - name: NUM_CUSTOMERS
              value: {{ .Values.seed.numCustomers | quote }}
            - name: NUM_ACCOUNTS
              value: {{ .Values.seed.numAccounts | quote }}
            - name: NUM_TRANSACTIONS
              value: {{ .Values.seed.numTransactions | quote }}
```

- [ ] **Step 5: Write `helm/trino-demo/templates/pipeline-runner.yaml`**

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: {{ .Release.Name }}-pipeline-runner
spec:
  schedule: {{ .Values.pipelineRunner.schedule | quote }}
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
            - name: pipeline-runner
              image: {{ .Values.pipelineRunner.image }}
              env:
                - name: TRINO_HOST
                  value: {{ .Release.Name }}-trino
                - name: TRINO_PORT
                  value: "8080"
                - name: PIPELINES_DIR
                  value: /pipelines
              volumeMounts:
                - name: pipelines
                  mountPath: /pipelines
          volumes:
            - name: pipelines
              configMap:
                name: {{ .Release.Name }}-pipelines
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-pipelines
data:
  customer_profile.yaml: |
{{ .Files.Get "../../pipelines/customer_profile.yaml" | indent 4 }}
  customer_profile.sql: |
{{ .Files.Get "../../pipelines/customer_profile.sql" | indent 4 }}
  monthly_spending.yaml: |
{{ .Files.Get "../../pipelines/monthly_spending.yaml" | indent 4 }}
  monthly_spending.sql: |
{{ .Files.Get "../../pipelines/monthly_spending.sql" | indent 4 }}
  high_value_customers.yaml: |
{{ .Files.Get "../../pipelines/high_value_customers.yaml" | indent 4 }}
  high_value_customers.sql: |
{{ .Files.Get "../../pipelines/high_value_customers.sql" | indent 4 }}
```

- [ ] **Step 6: Write `helm/trino-demo/templates/ui.yaml`**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-ui-config
data:
  default.conf: |
    server {
      listen {{ .Values.ui.port }};
      root /usr/share/nginx/html;
      index index.html;
      location /api/trino/ {
        proxy_pass http://{{ .Release.Name }}-trino:8080/;
        proxy_set_header X-Trino-User admin;
        proxy_read_timeout 120s;
      }
      location / { try_files $uri /index.html; }
    }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Release.Name }}-ui
spec:
  replicas: 1
  selector:
    matchLabels:
      app: {{ .Release.Name }}-ui
  template:
    metadata:
      labels:
        app: {{ .Release.Name }}-ui
    spec:
      containers:
        - name: ui
          image: {{ .Values.ui.image }}
          ports:
            - containerPort: {{ .Values.ui.port }}
          volumeMounts:
            - name: html
              mountPath: /usr/share/nginx/html/index.html
              subPath: index.html
            - name: nginx-conf
              mountPath: /etc/nginx/conf.d/default.conf
              subPath: default.conf
      volumes:
        - name: html
          configMap:
            name: {{ .Release.Name }}-ui-html
        - name: nginx-conf
          configMap:
            name: {{ .Release.Name }}-ui-config
---
apiVersion: v1
kind: Service
metadata:
  name: {{ .Release.Name }}-ui
spec:
  selector:
    app: {{ .Release.Name }}-ui
  ports:
    - port: {{ .Values.ui.port }}
      targetPort: {{ .Values.ui.port }}
```

- [ ] **Step 7: Commit**

```bash
git add helm/
git commit -m "feat: helm umbrella chart (nessie, seed-job, pipeline-runner, ui)"
```

---

## Task 10: End-to-end smoke test (Docker Compose)

- [ ] **Step 1: Build images**

```bash
cd ~/src/trino-demo
docker compose build seed pipeline-runner
```

Expected: both build successfully with no errors.

- [ ] **Step 2: Start infrastructure**

```bash
docker compose up -d minio minio-init nessie trino ui
```

- [ ] **Step 3: Wait for Trino to be ready**

```bash
until curl -sf http://localhost:8080/v1/info | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if not d.get('starting',True) else 1)"; do echo "waiting for trino…"; sleep 5; done
echo "Trino ready"
```

- [ ] **Step 4: Run seed job**

```bash
docker compose run --rm --profile tools seed
```

Expected output:

```
Nessie ready
Generating data…
  created silver.customers: 1000 rows
  created silver.accounts: 1500 rows
  created silver.transactions: 50000 rows
Seeding complete.
```

- [ ] **Step 5: Verify silver tables exist in Trino**

```bash
curl -s -X POST http://localhost:8080/v1/statement \
  -H 'X-Trino-User: admin' \
  -H 'Content-Type: text/plain' \
  -d 'SHOW TABLES IN iceberg.silver' | python3 -c "
import sys, json, time, urllib.request
result = json.load(sys.stdin)
while 'nextUri' in result:
    time.sleep(0.3)
    result = json.loads(urllib.request.urlopen(result['nextUri']).read())
print([r[0] for r in result.get('data', [])])
"
```

Expected: `['accounts', 'customers', 'transactions']`

- [ ] **Step 6: Run pipeline runner**

```bash
docker compose run --rm --profile tools pipeline-runner
```

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

- [ ] **Step 7: Open UI and verify all views**

```bash
open http://localhost:3000
```

Check each view:

- **Sources**: three silver cards show row counts (not `—`)
- **Pipelines**: canvas renders with bezier edges connecting nodes; click a pipeline node → right panel shows YAML + SQL
- **Catalog**: click `silver.customers` → columns and 10-row sample appear
- **Query**: run `SELECT COUNT(*) FROM iceberg.gold.customer_profile` → returns 1000
- **Insights**: KPI cards populated; spending category bar chart visible; HVC table shows customers

- [ ] **Step 8: Final commit**

```bash
git add .
git commit -m "feat: complete trino demo — seed, pipelines, UI, helm"
```

---

## Self-review

**Spec coverage:**

- ✅ Docker Compose local dev (Task 3, 10)
- ✅ Helm k8s deployment (Task 9)
- ✅ Silver layer (customers, accounts, transactions) seeded via PyIceberg (Task 4)
- ✅ Three gold pipelines as SQL+YAML data-as-code (Task 5)
- ✅ Pipeline runner with topo sort + Trino REST (Task 6)
- ✅ Nessie Iceberg REST catalog (Task 3 compose, Task 9 helm)
- ✅ Dark-themed HTML UI — Sources, Pipelines, Catalog, Query, Insights (Task 8)
- ✅ nginx proxy to Trino REST API (Task 7)
- ✅ End-to-end smoke test (Task 10)

**No placeholders found.**

**Type consistency:** `build_ctas_sql(target, sql, strategy)` defined in Task 6 step 4, tested in Task 6 step 2 — consistent. `load_pipelines(directory)` and `topological_sort(pipelines)` consistent throughout.
