{{ config(
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['order_number', 'product_number'],
    on_schema_change = 'sync_all_columns'
)
}}


WITH filtered AS (
    SELECT
        sd.sls_ord_num  AS order_number,
        sd.sls_prd_key  AS product_number,
        sd.sls_cust_id  AS customer_id,
        sd.sls_order_dt AS order_date,
        sd.sls_ship_dt  AS shipping_date,
        sd.sls_due_dt   AS due_date,
        sd.sls_sales    AS sales_amount,
        sd.sls_quantity AS quantity,
        sd.sls_price    AS price,
        sd.updated_at   AS updated_at
    FROM {{ ref('sales_details') }} AS sd
    {% if is_incremental() %}
    WHERE sd.updated_at >= (
        COALESCE(
            (SELECT MAX(t.updated_at) FROM {{ this }} AS t),
            TIMESTAMP '1900-01-01'
        ) - INTERVAL '3 DAYS'
    )
    {% endif %}
)
SELECT
    f.order_number,
    pr.product_key    AS product_key,
    cu.customer_key   AS customer_key,
    f.order_date,
    f.shipping_date,
    f.due_date,
    f.sales_amount,
    f.quantity,
    f.price,
    f.updated_at,
    CURRENT_TIMESTAMP  AS gold_loaded_at
FROM filtered AS f
LEFT JOIN {{ ref('dim_products') }} AS pr
    ON f.product_number = pr.product_number
LEFT JOIN {{ ref('dim_customers') }} AS cu
    ON CAST(CAST(f.customer_id AS DOUBLE) AS BIGINT) = CAST(CAST(cu.customer_id AS DOUBLE) AS BIGINT)