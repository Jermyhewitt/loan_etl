{{
    config(
        materialized='view'
    )
}}

select
    COUNT(loan_repayment.id) AS number_of_repayments,
    SUM(loan_repayment.amount) as total_payment,
    DATE_FORMAT(created_at, '%Y-%m') AS repayment_month
FROM {{ ref('loan_repayment')}} as loan_repayment
group by repayment_month
order by repayment_month