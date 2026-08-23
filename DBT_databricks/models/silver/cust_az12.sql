{{ config(
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = '_id',
    on_schema_change = 'sync_all_columns'
)
}}


WITH incremental_filter AS(
    SELECT
        BDATE,
        CID,
        GEN,
        _id,
        updated_at
    FROM {{ source('bronze', 'CUST_AZ12')}}
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
        CASE
            WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
            ELSE CID
        END AS cid,
        CASE
            WHEN BDATE > CURRENT_DATE() THEN NULL
            ELSE BDATE
        END AS bdate,
        CASE
            WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE')
                THEN 'Female'
            WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE')
                THEN 'Male'
            ELSE 'N/A'
        END AS gen,
        _id,
        updated_at
    FROM incremental_filter
)
SELECT
    _id,
    cid                 :: STRING    AS cid,
    bdate               :: STRING    AS bdate,
    gen                 :: STRING    AS gen,
    updated_at          :: TIMESTAMP AS updated_at,
    CURRENT_TIMESTAMP                AS silver_loaded_at
FROM cleaned