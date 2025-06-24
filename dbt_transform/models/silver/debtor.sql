{{
    config(
        materialized='incremental',
        unique_key='loan_id'
    )
}}

select
    firstname as first_name,
    lastname as last_name,
    email,
    user.telephone as phone_number,
    loan.id as loan_id,
    loan.approvedDate as approved_at
from {{ source('etl_source', 'debtor') }} as debtor
join {{ source('etl_source', 'user') }} as user on debtor.user_id = user.id
join {{ source('etl_source', 'user_address') }} as user_address on user.id = user_address.userId
join {{ source('etl_source', 'address') }} as address on user_address.addressId = address.id 
join {{ source('etl_source', 'loan') }} as loan on debtor.loan_id = loan.id

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  -- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
where loan.approvedDate  >= (select coalesce(max(approved_at),'1900-01-01') from {{ this }} )

{% endif %}
