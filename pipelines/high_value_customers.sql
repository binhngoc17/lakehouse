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
