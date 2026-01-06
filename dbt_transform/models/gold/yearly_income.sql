{{
    config(
        materialized='view'
    )
}}

select
    COUNT(loan_repayment.loan_repayment_id) AS number_of_repayments,
    SUM(loan_repayment.amount) as total_payment,
    EXTRACT(YEAR FROM loan_repayment.created_at) as repayment_year
FROM {{ ref('loan_repayment')}} as loan_repayment
group by repayment_year
order by repayment_year