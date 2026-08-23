{{ config(
    materialized = 'incremental',
    file_format = 'delta',
    unique_key = '_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
)
}}


WITH incremental_filter AS(
    SELECT
        CID,
        CNTRY,
        _id,
        updated_at
    FROM {{ (source('bronze', 'LOC_A101'))}}
    WHERE _id IS NOT NULL
    {% if is_incremental() %}
            AND COALESCE(updated_at :: TIMESTAMP, TIMESTAMP '1900-01-01') >= (
                COALESCE(
                    (SELECT MAX(t.updated_at :: TIMESTAMP) FROM {{ this }} AS t),
                    TIMESTAMP '1900-01-01'
                ) - INTERVAL '3 DAYS'
            )
    {% endif %}
),
cleaned AS(
    SELECT
        REPLACE(CID,'-','') AS cid,
        CASE
            WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
            WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'N/A'
            ELSE TRIM(CNTRY)
        END AS cntry,
        _id,
        updated_at
    FROM incremental_filter
)
SELECT
    _id,
    cid         :: STRING    AS cid,
    cntry       :: STRING    AS cntry,
    updated_at  :: TIMESTAMP AS updated_at,
    CURRENT_TIMESTAMP        AS silver_loaded_at
FROM cleaned