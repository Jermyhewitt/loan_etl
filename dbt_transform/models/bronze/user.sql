{{
    config(
        materialized='view'
    )
}}

select
    id,
    email,
    firstname as first_name,
    lastname as last_name,
    role,
    createdAt as created_at,
    updatedAt as updated_at
from {{ source('etl_source', 'user') }} 