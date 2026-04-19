-- depends_on: {{ ref('bronze_accounts') }}

select
    account_id,
    account_name,
    industry,
    country,
    cast(signup_date as date) as signup_date,
    referral_source,
    plan_tier as initial_plan_tier,
    seats as account_seats,
    is_trial,
    churn_flag
from {{ ref("bronze_accounts") }}
