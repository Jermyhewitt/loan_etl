{{
    config(
        materialized='view'
    )
}}

SELECT
  SUM(principal) AS total_principal,
  COUNT(id) AS total_loans,
 EXTRACT(YEAR FROM approved_at) as approved_year
FROM {{ ref('loan') }}
WHERE approved_at IS NOT NULL
GROUP BY approved_year
ORDER BY approved_year