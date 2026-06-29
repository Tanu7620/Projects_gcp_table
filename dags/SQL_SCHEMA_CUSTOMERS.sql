INSERT INTO `bigquery7620.datasetddl.sales_data`
(
    customer_name,
    customer_id,
    quantity,
    salary,
    created_at,
    lastingestiontime
)

SELECT
    trim(SAFE_CAST(customer_name  AS STRING))   AS customer_name,
    SAFE_CAST(customer_id    AS INT64)     AS customer_id,
    SAFE_CAST(quantity       AS NUMERIC)   AS quantity,
    SAFE_CAST(salary         AS FLOAT64)   AS salary,
    SAFE_CAST(created_at     AS TIMESTAMP) AS created_at,
    CURRENT_TIMESTAMP()                    AS lastingestiontime

FROM `bigquery7620.datasetddl.sales_data`
