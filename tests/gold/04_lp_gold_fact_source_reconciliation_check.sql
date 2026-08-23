-- Loops (gold_fact, silver_source) pairs and compares row counts. fct_sales
-- should have exactly one row per sales_details row -- a LEFT JOIN to
-- dim_customers/dim_products should never add or drop rows unless one of
-- those dims has a duplicate natural key (fan-out) which 15_ would also
-- catch. A gold count LOWER than silver would mean rows were dropped,
-- which a LEFT JOIN shouldn't do either -- worth investigating either way.
-- Note: run after a full backfill, not mid-incremental-window, to avoid
-- comparing partially-loaded counts. Returns empty when they match.

BEGIN

    DECLARE gold_count   BIGINT;
    DECLARE source_count BIGINT;

    FOR reconcile_cursor AS
        SELECT * FROM (VALUES
            ('fct_sales', 'sales_details')
        ) AS t(gold_table, silver_table)
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*) FROM datawarehouse.gold.`' || reconcile_cursor.gold_table || '`'
        INTO gold_count;

        EXECUTE IMMEDIATE
            'SELECT COUNT(*) FROM datawarehouse.silver.`' || reconcile_cursor.silver_table || '`'
        INTO source_count;

        IF gold_count <> source_count THEN
            SELECT
                reconcile_cursor.gold_table   AS gold_table,
                reconcile_cursor.silver_table AS silver_table,
                gold_count                    AS gold_row_count,
                source_count                  AS silver_row_count,
                gold_count - source_count      AS row_count_delta;
        END IF;

    END FOR;

END;