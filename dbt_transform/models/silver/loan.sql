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
    loan.approvedDate as approved_at,
    flatRate as flat_rate,
    type
from {{ source('etl_source', 'loan') }}
