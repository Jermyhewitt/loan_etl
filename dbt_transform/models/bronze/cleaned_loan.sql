{{
    config(
        materialized='view'
    )
}}

select
    loan.id,
    loan.principal,
    loan.interest as interest_rate,
    loan.period,
    loan.status,
    loan.createdAt as created_at,
    loan.updatedAt as updated_at,
    loan.approvedDate as approved_at
from {{ source('etl_source', 'loan') }}
where loan.status = 'approved'
