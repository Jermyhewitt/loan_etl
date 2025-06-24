{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

select
    user.firstname as first_name,
    user.lastname as last_name,
    loan_repayment.id,
    loan_id,
    user_id,
    transaction_id,
    loan_repayment.createdAt as created_at,
    loan_repayment.updatedAt as updated_at,
    loan_repayment.amount,
    loan_repayment.systemFee,
    loan_repayment.interest,
    loan_repayment.principal,
    loan_repayment.lateFee,
    loan_repayment.refinancedInterest
from {{ source('etl_source', 'loan_repayment') }} as loan_repayment
join {{ source('etl_source', 'loan') }} as loan on loan_repayment.loan_id = loan.id
join {{ source('etl_source', 'user') }} as user on loan_repayment.user_id = user.id

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  -- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
where loan_repayment.createdAt  >= (select coalesce(max(created_at),'1900-01-01') from {{ this }} )

{% endif %}