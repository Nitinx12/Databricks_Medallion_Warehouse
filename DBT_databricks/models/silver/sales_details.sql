{{ config(
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['sls_ord_num', 'sls_prd_key'],
    on_schema_change = 'sync_all_columns'
)
}}

WITH incremental_filter AS (
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price,
        updated_at
    FROM {{ source('bronze', 'sales_details') }}
    WHERE sls_ord_num IS NOT NULL
    {% if is_incremental() %}
        AND COALESCE(updated_at :: TIMESTAMP, TIMESTAMP '1900-01-01') >= (
            COALESCE(
                (SELECT MAX(t.updated_at :: TIMESTAMP) FROM {{ this }} AS t),
                TIMESTAMP '1900-01-01'
            ) - INTERVAL '3 DAYS'
        )
    {% endif %}
),
cleaned AS (
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt = 0 OR LEN(CAST(sls_order_dt AS STRING)) <> 8
                THEN NULL
            ELSE TRY_TO_DATE(CAST(sls_order_dt AS STRING), 'yyyyMMdd')
        END AS sls_order_dt,
        CASE
            WHEN sls_ship_dt = 0 OR LEN(CAST(sls_ship_dt AS STRING)) <> 8
                THEN NULL
            ELSE TRY_TO_DATE(CAST(sls_ship_dt AS STRING), 'yyyyMMdd')
        END AS sls_ship_dt,
        CASE
            WHEN sls_due_dt = 0 OR LEN(CAST(sls_due_dt AS STRING)) <> 8
                THEN NULL
            ELSE TRY_TO_DATE(CAST(sls_due_dt AS STRING), 'yyyyMMdd')
        END AS sls_due_dt,
        TRY_CAST(sls_quantity AS INT) AS sls_quantity_int,
        CASE
            WHEN UPPER(TRIM(sls_sales)) IN ('NAN', 'INFINITY', '-INFINITY')
                THEN NULL
            ELSE TRY_CAST(sls_sales AS DECIMAL(10,2))
        END AS sls_sales_raw,
        CASE
            WHEN UPPER(TRIM(sls_price)) IN ('NAN', 'INFINITY', '-INFINITY')
                THEN NULL
            ELSE TRY_CAST(sls_price AS DECIMAL(10,2))
        END AS sls_price_raw,
        CASE
            WHEN sls_price_raw IS NULL OR sls_price_raw <= 0
                THEN sls_sales_raw / NULLIF(sls_quantity_int, 0)
            ELSE sls_price_raw
        END AS sls_price_resolved,
        CASE
            WHEN sls_sales_raw IS NULL
                OR sls_sales_raw <= 0
                OR sls_sales_raw <> sls_quantity_int * ABS(sls_price_resolved)
                THEN sls_quantity_int * ABS(sls_price_resolved)
            ELSE sls_sales_raw
        END AS sls_sales_resolved,
        updated_at
    FROM incremental_filter
)
SELECT
    sls_ord_num                                     :: STRING          AS sls_ord_num,
    sls_prd_key                                     :: STRING          AS sls_prd_key,
    sls_cust_id                                     :: STRING          AS sls_cust_id,
    sls_order_dt                                    :: DATE            AS sls_order_dt,
    sls_ship_dt                                     :: DATE            AS sls_ship_dt,
    sls_due_dt                                      :: DATE            AS sls_due_dt,
    TRY_CAST(sls_price_resolved AS INT)                                AS sls_price,
    TRY_CAST(sls_quantity_int AS INT)                                  AS sls_quantity,
    TRY_CAST(sls_sales_resolved AS DECIMAL(10,2))                      AS sls_sales,
    updated_at                                      :: TIMESTAMP       AS updated_at,
    CURRENT_TIMESTAMP                      AS silver_loaded_at
FROM cleaned