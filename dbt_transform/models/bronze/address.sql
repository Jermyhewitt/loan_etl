{{
    config(
        materialized='view'
    )
}}

select
    id,
    building,
    street,
    city,
    state,
    country
from {{ source('etl_source', 'address') }} 