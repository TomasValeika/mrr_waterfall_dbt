select account_id, month_start, count(*) as row_count
from {{ ref("silver_account_month_mrr") }}
group by account_id, month_start
having count(*) > 1
