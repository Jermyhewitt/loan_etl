{{
    config(
        materialized='view'
    )
}}

SELECT
  SUM(principal) AS total_principal,
  COUNT(id) AS total_loans,
  DATE_FORMAT(approved_at, '%Y-%m') AS approved_month
FROM {{ ref('loan') }}
WHERE approved_at IS NOT NULL
GROUP BY approved_month
ORDER BY approved_month
