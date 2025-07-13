{{ 
    config(
        materialized='table'
    ) 
}}

WITH payday_loans AS (
  SELECT
    id,
    principal,
    DATE_FORMAT(approved_at, '%Y-%m') AS approved_month,
    flat_rate * period AS total_fee
  FROM {{ ref('loan') }}
  WHERE approved_at IS NOT NULL
    AND type = 'payday'
),

personal_loans AS (
  SELECT
    id,
    principal,
    DATE_FORMAT(approved_at, '%Y-%m') AS approved_month,
    principal * interest_rate  AS total_fee
  FROM {{ ref('loan') }}
  WHERE approved_at IS NOT NULL
    AND type = 'personal'
),

all_loans AS (
  SELECT * FROM payday_loans
  UNION ALL
  SELECT * FROM personal_loans
)

SELECT
  approved_month,
  COUNT(*) AS total_loans,
  SUM(principal) AS total_principal,
  SUM(total_fee) AS total_profit
FROM all_loans
GROUP BY approved_month
ORDER BY approved_month