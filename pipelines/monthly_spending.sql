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
