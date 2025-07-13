{{
    config(
        materialized='view'
    )
}}

SELECT
  SUM(principal) AS total_principal,
  COUNT(id) AS total_loans,
  EXTRACT(YEAR FROM approved_at) as approved_year,
  SUM(payment) AS total_payment,
  SUM(payment)-SUM(principal)   AS loss
FROM {{ ref('loan') }}
JOIN (
    SELECT
        loan_id,
        SUM(amount) AS payment
    FROM {{ ref('loan_repayment') }}
    GROUP BY loan_id
) AS bad_debt ON bad_debt.loan_id = id
WHERE status='bad debt'
GROUP BY approved_year
ORDER BY approved_year
