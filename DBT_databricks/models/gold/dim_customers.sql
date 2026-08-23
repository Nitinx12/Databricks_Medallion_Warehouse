{{ config(
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = 'customer_id',
    on_schema_change = 'sync_all_columns'
)
}}


WITH joined AS (
    SELECT
        CI.cst_id                                        AS customer_id,
        CI.cst_key                                        AS customer_number,
        CONCAT(CI.cst_first_name, ' ', CI.cst_last_name)  AS customer_name,
        LA.cntry                                          AS country,
        CI.cst_marital_status                             AS marital_status,
        CASE
            WHEN CI.cst_gndr <> 'N/A'
                THEN CI.cst_gndr
            ELSE COALESCE(CA.gen, 'N/A')
        END                                                AS gender,
        CA.bdate                                           AS birthdate,
        CI.cst_create_date                                 AS create_date,
        GREATEST(CI.updated_at, CA.updated_at, LA.updated_at) AS updated_at
    FROM {{ ref('cust_info') }} AS CI
    LEFT JOIN {{ ref('cust_az12') }} AS CA
        ON CI.cst_key = CA.cid
    LEFT JOIN {{ ref('loc_a101') }} AS LA
        ON LA.cid = CI.cst_key
    {% if is_incremental() %}
    WHERE GREATEST(CI.updated_at, CA.updated_at, LA.updated_at) >= (
        COALESCE(
            (SELECT MAX(t.updated_at) FROM {{ this }} AS t),
            TIMESTAMP '1900-01-01'
        ) - INTERVAL '3 DAYS'
    )
    {% endif %}
)
SELECT
    xxhash64(customer_id) AS customer_key,
    customer_id,
    customer_number,
    customer_name,
    country,
    marital_status,
    gender,
    birthdate,
    create_date,
    updated_at,
    CURRENT_TIMESTAMP     AS gold_loaded_at
FROM joined