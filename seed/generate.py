import os
import time
import random
import urllib.request
from datetime import date, timedelta, datetime
from decimal import Decimal, ROUND_HALF_UP

import pyarrow as pa
from faker import Faker
from pyiceberg.catalog import load_catalog

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


def _to_dec(val, precision=18, scale=2):
    if val is None:
        return None
    fmt = "." + "0" * scale
    return Decimal(str(val)).quantize(Decimal(fmt), rounding=ROUND_HALF_UP)


def _customers_to_arrow(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=CUSTOMER_ARROW)


def _accounts_to_arrow(rows: list[dict]) -> pa.Table:
    coerced = [
        {**r, "balance": _to_dec(r["balance"]), "credit_limit": _to_dec(r["credit_limit"])}
        for r in rows
    ]
    return pa.Table.from_pylist(coerced, schema=ACCOUNT_ARROW)


def _transactions_to_arrow(rows: list[dict]) -> pa.Table:
    coerced = [
        {**r, "amount": _to_dec(r["amount"]), "fx_rate": _to_dec(r["fx_rate"], scale=6)}
        for r in rows
    ]
    return pa.Table.from_pylist(coerced, schema=TRANSACTION_ARROW)


def _wait_for_nessie(uri: str, retries: int = 30, delay: int = 3) -> None:
    health = uri.rstrip("/") + "/v1/config"
    for i in range(retries):
        try:
            urllib.request.urlopen(health, timeout=3)
            print("Nessie ready")
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


def _write_table(catalog, identifier: str, arrow_table: pa.Table) -> None:
    try:
        tbl = catalog.load_table(identifier)
        tbl.overwrite(arrow_table)
        print(f"  overwritten {identifier}: {len(arrow_table)} rows")
    except Exception:
        tbl = catalog.create_table(identifier, schema=arrow_table.schema)
        tbl.append(arrow_table)
        print(f"  created {identifier}: {len(arrow_table)} rows")


def seed(catalog=None) -> None:
    _wait_for_nessie(NESSIE_URI)
    if catalog is None:
        catalog = _load_catalog()

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


if __name__ == "__main__":
    seed()
