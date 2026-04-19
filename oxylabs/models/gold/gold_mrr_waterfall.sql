-- depends_on: {{ ref('silver_account_month_mrr') }}

with
    account_months as (

        select account_id, month_start, mrr from {{ ref("silver_account_month_mrr") }}

    ),

    account_history as (

        select
            account_id,
            month_start,
            mrr as current_mrr,
            lag(mrr, 1, 0) over (
                partition by account_id order by month_start
            ) as previous_mrr,
            max(case when mrr > 0 then 1 else 0 end) over (
                partition by account_id
                order by month_start
                rows between unbounded preceding and 1 preceding
            ) as had_positive_mrr_before
        from account_months

    ),

    classified as (

        select
            month_start,
            previous_mrr,
            current_mrr,

            case
                when
                    previous_mrr = 0
                    and current_mrr > 0
                    and coalesce(had_positive_mrr_before, 0) = 0
                then current_mrr
                else 0
            end as new_mrr,

            case
                when
                    previous_mrr = 0
                    and current_mrr > 0
                    and coalesce(had_positive_mrr_before, 0) = 1
                then current_mrr
                else 0
            end as reactivation_mrr,

            case
                when previous_mrr > 0 and current_mrr > previous_mrr
                then current_mrr - previous_mrr
                else 0
            end as expansion_mrr,

            case
                when previous_mrr > 0 and current_mrr > 0 and current_mrr < previous_mrr
                then previous_mrr - current_mrr
                else 0
            end as contraction_mrr,

            case
                when previous_mrr > 0 and current_mrr = 0 then previous_mrr else 0
            end as churn_mrr
        from account_history

    ),

    monthly as (

        select
            month_start as month,
            sum(previous_mrr) as opening_mrr,
            sum(new_mrr) as new_mrr,
            sum(reactivation_mrr) as reactivation_mrr,
            sum(expansion_mrr) as expansion_mrr,
            sum(contraction_mrr) as contraction_mrr,
            sum(churn_mrr) as churn_mrr,
            sum(current_mrr) as closing_mrr
        from classified
        group by 1

    )

select
    month,
    opening_mrr,
    new_mrr,
    reactivation_mrr,
    expansion_mrr,
    contraction_mrr,
    churn_mrr,
    closing_mrr
from monthly
order by month
