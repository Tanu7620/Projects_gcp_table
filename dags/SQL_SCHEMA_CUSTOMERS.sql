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

    SAFE_CAST(customer_name  AS STRING)    AS customer_name,
    -- ↑ converts to text safely

    SAFE_CAST(customer_id    AS INT64)     AS customer_id,
    -- ↑ converts to whole number safely

    SAFE_CAST(quantity       AS NUMERIC)   AS quantity,
    -- ↑ converts to decimal number safely

    SAFE_CAST(salary         AS FLOAT64)   AS salary,
    -- ↑ converts to float number safely

    SAFE_CAST(created_at     AS TIMESTAMP) AS created_at,
    -- ↑ converts to date and time safely

    CURRENT_TIMESTAMP()                    AS lastingestiontime
    -- ↑ automatically captures date and time of ingestion
    -- no need to update separately!

FROM `bigquery7620.datasetddl.staging_sales_data`
-- ↑ this is your SOURCE table where raw data lands first from GCS
-- then this SQL cleans and inserts into your target table
