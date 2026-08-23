{{ config(
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = 'product_id',
    on_schema_change = 'sync_all_columns'
)
}}


WITH joined AS (
    SELECT
        PN.prd_id                              AS product_id,
        PN.prd_key                             AS product_number,
        PN.prd_nm                              AS product_name,
        PN.cat_id                              AS category_id,
        COALESCE(PC.cat, 'N/A')                AS category,
        COALESCE(PC.subcat, 'N/A')             AS subcategory,
        PC.maintenance                         AS maintenance,
        PN.prd_cost                            AS cost,
        PN.prd_line                            AS product_line,
        PN.prd_start_dt                        AS start_date,
        GREATEST(PN.updated_at, PC.updated_at) AS updated_at
    FROM {{ ref('prd_info') }} AS PN
    LEFT JOIN {{ ref('px_cat_g1v2') }} AS PC
        ON PN.cat_id = PC.id
    WHERE PN.prd_end_dt IS NULL  -- current version of each product only
    {% if is_incremental() %}
    AND GREATEST(PN.updated_at, PC.updated_at) >= (
        COALESCE(
            (SELECT MAX(t.updated_at) FROM {{ this }} AS t),
            TIMESTAMP '1900-01-01'
        ) - INTERVAL '3 DAYS'
    )
    {% endif %}
)
SELECT
    xxhash64(product_id) AS product_key,
    product_id,
    product_number,
    product_name,
    category_id,
    category,
    subcategory,
    maintenance,
    cost,
    product_line,
    start_date,
    updated_at,
    CURRENT_TIMESTAMP    AS gold_loaded_at
FROM joined