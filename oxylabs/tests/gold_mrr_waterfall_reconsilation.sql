select *
from {{ ref("gold_mrr_waterfall") }}
where
    opening_mrr
    + new_mrr
    + reactivation_mrr
    + expansion_mrr
    - contraction_mrr
    - churn_mrr
    != closing_mrr
