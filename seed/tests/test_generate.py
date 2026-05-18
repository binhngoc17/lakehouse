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
