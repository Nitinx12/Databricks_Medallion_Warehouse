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
        CAT,
        ID,
        MAINTENANCE,
        SUBCAT,
        _id,
        updated_at
    FROM {{ (source('bronze', 'PX_CAT_G1V2')) }}
    WHERE _id IS NOT NULL
        {% if is_incremental() %}
                AND COALESCE(updated_at :: TIMESTAMP, TIMESTAMP '1900-01-01') >= (
                    COALESCE(
                        (SELECT MAX(t.updated_at :: TIMESTAMP) FROM {{ this }} AS t),
                        TIMESTAMP '1900-01-01'
                    ) - INTERVAL '3 DAYS'
                )
        {% endif %}
)
SELECT
   _id,
   CAT         :: STRING AS cat,
   ID          :: STRING AS id,
   SUBCAT      :: STRING AS subcat,
   MAINTENANCE :: STRING AS maintenance,
   updated_at  :: TIMESTAMP AS updated_at,
   CURRENT_TIMESTAMP        AS silver_loaded_at
FROM incremental_filter