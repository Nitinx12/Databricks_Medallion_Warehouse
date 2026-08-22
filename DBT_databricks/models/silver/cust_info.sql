{{ config(
    materialized = 'incremental',
    file_format = 'delta',
    unique_key = 'cst_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
)
}}

WITH incremental_filter AS (
    SELECT
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date,
        updated_at
    FROM {{ source('bronze', 'cust_info') }}
    WHERE cst_id IS NOT NULL
        AND cst_create_date != 'NaN'
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
            PARTITION BY cst_id
            ORDER BY
                updated_at DESC NULLS LAST,
                cst_create_date DESC NULLS LAST
        ) AS rnk
    FROM incremental_filter
),
cleaned AS (
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_first_name,
        TRIM(cst_lastname)  AS cst_last_name,
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'N/A'
        END AS cst_marital_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'N/A'
        END AS cst_gndr,
        TRY_CAST(cst_create_date AS DATE) AS cst_create_date,
        TRY_CAST(updated_at AS TIMESTAMP) AS updated_at
    FROM duplicated
    WHERE rnk = 1
)
SELECT
    cst_id :: STRING                   AS cst_id,
    cst_key :: STRING                  AS cst_key,
    cst_first_name :: STRING           AS cst_first_name,
    cst_last_name :: STRING            AS cst_last_name,
    cst_marital_status :: STRING       AS cst_marital_status,
    cst_gndr :: STRING                 AS cst_gndr,
    cst_create_date                    AS cst_create_date,
    updated_at                         AS updated_at,
    CURRENT_TIMESTAMP                  AS silver_loaded_at
FROM cleaned