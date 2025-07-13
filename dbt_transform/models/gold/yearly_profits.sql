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
),
bad_debt as (
    SELECT
        id,
        principal,
        principal-total_payment as bad_debt
    FROM {{ ref('loan') }}
    LEFT JOIN monthly_payments mp on mp.loan_id = id
    WHERE status='bad debt'
),
current_loans AS (
    SELECT
        id,
        principal - total_payment as remaining_principal
    FROM {{ ref('loan') }} l 
    LEFT JOIN monthly_payments mp ON mp.loan_id = l.id
    WHERE full_repayment = 0
    and status ="approved"
)

SELECT
  EXTRACT(YEAR FROM l.approved_at) as approved_year,
  COUNT(*) AS total_loans,
  SUM(l.principal) AS total_principal,
  SUM(mp.total_payment) AS total_payment,
  SUM(mp.total_payment) - SUM(l.principal) AS profit_loss,
  SUM(bd.bad_debt) AS total_bad_debt,
  SUM(cl.remaining_principal) AS total_remaining_principal
FROM {{ ref('loan') }} l 
LEFT JOIN monthly_payments mp  ON mp.loan_id = l.id
LEFT JOIN bad_debt bd ON bd.id = l.id
LEFT JOIN current_loans cl ON cl.id = l.id
where status in ("refinanced","approved","bad debt")
GROUP BY approved_year
ORDER BY approved_year
