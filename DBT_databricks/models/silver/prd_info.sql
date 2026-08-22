{{ config(
    materialized = 'incremental',
    file_format = 'delta',
    unique_key = 'prd_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
)
}}
WITH incremental_filter AS (
    SELECT
        prd_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt,
        updated_at
    FROM {{ source('bronze', 'prd_info') }}
    WHERE prd_id IS NOT NULL
        AND prd_start_dt != 'NaN'
    {% if is_incremental() %}
        AND updated_at :: TIMESTAMP >= (
            COALESCE(
                (SELECT MAX(t.updated_at :: TIMESTAMP) FROM {{ this }} AS t),
                TIMESTAMP '1900-01-01'
            ) - INTERVAL '3 DAYS'
        )
    {% endif %}
),
duplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY prd_id
            ORDER BY
                prd_start_dt DESC NULLS LAST,
                updated_at DESC NULLS LAST
        ) AS rnk
    FROM incremental_filter
),
cleaned AS (
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')             AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key))                     AS prd_key,
        TRIM(prd_nm)                                            AS prd_nm,
        COALESCE(TRY_CAST(prd_cost AS DECIMAL(10,2)), 0)        AS prd_cost,
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'N/A'
        END                                                     AS prd_line,
        TRY_CAST(prd_start_dt AS DATE)                          AS prd_start_dt,
        TRY_CAST(
            LEAD(TRY_CAST(prd_start_dt AS DATE)) OVER (
                PARTITION BY prd_key
                ORDER BY TRY_CAST(prd_start_dt AS DATE)
            ) - INTERVAL 1 DAY AS DATE
        )                                                       AS prd_end_dt,
        TRY_CAST(updated_at AS TIMESTAMP)                       AS updated_at
    FROM duplicated
    WHERE rnk = 1
)
SELECT
    prd_id :: STRING            AS prd_id,
    cat_id :: STRING            AS cat_id,
    prd_key :: STRING           AS prd_key,
    prd_nm  :: STRING           AS prd_nm,
    prd_cost :: DECIMAL(10,2)   AS prd_cost,
    prd_line :: STRING          AS prd_line,
    prd_start_dt                AS prd_start_dt,
    prd_end_dt                  AS prd_end_dt,
    updated_at                  AS updated_at,
    CURRENT_TIMESTAMP           AS silver_loaded_at
FROM cleaned