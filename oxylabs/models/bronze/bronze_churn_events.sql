select * from {{ source("raw", "churn_events") }}
