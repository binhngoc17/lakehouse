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
