{{
    config(
        materialized='incremental',
        unique_key='loan_repayment_id'
    )
}}

select
    transferDate as transfer_date,
    user.firstname as first_name,
    user.lastname as last_name,
    loan_repayment.id as loan_repayment_id,
    loan.id as loan_id,
    user.id as user_id,
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
join {{ source('etl_source', 'user_deposit') }} as user_deposit on user_deposit.loan_id = loan.id
where user_deposit.purpose = "repayment"
    and user_deposit.status = "confirmed"

{% if is_incremental() %}
    and loan_repayment.createdAt >= (
            select coalesce(max(created_at), '1900-01-01')
            from {{ this }}
    )
{% endif %}