-- models/gold/active_loans.sql

{{ config(materialized='view') }}

SELECT
    first_name,
    last_name,
    id,
    l.principal,
    l.interest_rate,
    l.period,
    l.status,
    l.created_at,
    l.updated_at,
    l.approved_at,
    l.flat_rate,
    l.type
FROM
    {{ ref('loan') }} l
    JOIN {{ref ('debtor') }} d ON l.id = d.loan_id
WHERE
    full_repayment = 0
    AND status = "approved"
ORDER BY approved_at DESC