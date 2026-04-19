select * from {{ ref("silver_account_month_mrr") }} where mrr < 0
