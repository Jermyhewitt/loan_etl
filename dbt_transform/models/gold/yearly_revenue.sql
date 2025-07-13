{{
    config(
        materialized='view'
    )
}}

SELECT
  repayment_summary.repayment_year,
  repayment_summary.number_of_repayments,
  repayment_summary.total_payment,
  loan_summary.total_loans,
  loan_summary.total_principal,
  repayment_summary.total_payment - loan_summary.total_principal AS profit_loss
FROM {{ ref('yearly_income') }} as repayment_summary
LEFT JOIN {{ ref('yearly_loans') }} as loan_summary
  ON repayment_summary.repayment_year = loan_summary.approved_year
ORDER BY repayment_summary.repayment_year