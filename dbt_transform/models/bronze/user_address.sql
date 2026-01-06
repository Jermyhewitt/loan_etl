{{
    config(
        materialized='view'
    )
}}

select

    user_address.userId as user_id,
    user_address.addressId as address_id

from {{ source('etl_source', 'user_address') }} as user_address 