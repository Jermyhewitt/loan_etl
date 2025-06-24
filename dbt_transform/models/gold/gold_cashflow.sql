{{
    config(
        materialized='view'
    )
}}

SELECT
  repayment_summary.repayment_month,
  repayment_summary.number_of_repayments,
  repayment_summary.total_payment,
  loan_summary.total_loans,
  loan_summary.total_principal
FROM {{ ref('monthly_income') }} as repayment_summary
LEFT JOIN {{ ref('monthly_loans') }} as loan_summary
  ON repayment_summary.repayment_month = loan_summary.approved_month
ORDER BY repayment_summary.repayment_month, repayment_summary.repayment_month