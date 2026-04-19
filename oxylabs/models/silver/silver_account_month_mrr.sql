-- depends_on: {{ ref('silver_subscriptions') }}

with
    min_max_month as (

        select
            date_trunc('month', min(start_date))::date as min_month,
            date_trunc('month', current_date)::date as max_month
        from {{ ref("silver_subscriptions") }}

    ),

    month_range as (

        select
            generate_series(min_month, max_month, interval '1 month')::date
            as month_start
        from min_max_month

    ),
    account_range as (select distinct account_id from {{ ref("silver_accounts") }}),

    account_month_range as (

        select * from account_range as a cross join month_range as m
    ),
    subscription_months as (

        select s.account_id, m.month_start, s.mrr_amount
        from {{ ref("silver_subscriptions") }} as s
        join
            month_range as m
            on m.month_start >= date_trunc('month', s.start_date)::date
            and (
                s.end_date is null
                or m.month_start <= date_trunc('month', s.end_date)::date
            )
        where coalesce(s.is_trial, false) = false
    ),

    monthly_mrr as (

        select account_id, month_start, sum(mrr_amount) as mrr
        from subscription_months
        group by account_id, month_start
    )

select mo_rng.account_id, mo_rng.month_start, coalesce(mrr.mrr, 0)::integer as mrr
from account_month_range as mo_rng
left join
    monthly_mrr as mrr
    on mo_rng.account_id = mrr.account_id
    and mo_rng.month_start = mrr.month_start
order by mo_rng.account_id, mo_rng.month_start
