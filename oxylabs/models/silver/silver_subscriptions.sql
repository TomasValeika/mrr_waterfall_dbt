-- depends_on: {{ ref('bronze_subscriptions') }}

select
    subscription_id,
    account_id,
    cast(start_date as date) as start_date,
    cast(end_date as date) as end_date,
    plan_tier,
    seats,
    mrr_amount,
    arr_amount,
    is_trial,
    upgrade_flag,
    downgrade_flag,
    churn_flag,
    billing_frequency,
    auto_renew_flag
from {{ ref("bronze_subscriptions") }}
