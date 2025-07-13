{{
    config(
        materialized='table'
    )
}}

WITH monthly_payments AS (
    SELECT
        loan_id,
        SUM(amount) AS total_payment
    FROM {{ ref('loan_repayment') }}
    GROUP BY loan_id
)

SELECT
  DATE_FORMAT(l.created_at, '%Y-%m') AS approved_month,
  COUNT(*) AS total_loans,
  SUM(l.principal) AS total_principal,
  SUM(mp.total_payment) AS total_payment,
  SUM(mp.total_payment) - SUM(l.principal) AS profit_loss,
FROM monthly_payments mp
JOIN {{ ref('loan') }} l ON mp.loan_id = l.id
GROUP BY approved_month
ORDER BY approved_month
