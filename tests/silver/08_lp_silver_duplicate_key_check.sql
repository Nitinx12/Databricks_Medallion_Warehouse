-- Config-driven loop: for each (table, key_columns) pair, checks whether
-- the key is actually unique after the model's dedup logic (ROW_NUMBER()
-- rnk = 1 filters, etc). Catches a dedup window function that stops
-- working as new duplicate source rows show up. Add new tables/keys to
-- the VALUES list as models are added. Returns empty when clean.

BEGIN

    DECLARE duplicate_count BIGINT;

    FOR key_cursor AS
        SELECT * FROM (VALUES
            ('cust_info',     'cst_id'),
            ('cust_az12',     '_id'),
            ('loc_a101',      '_id'),
            ('prd_info',      'prd_id'),
            ('px_cat_g1v2',   '_id'),
            ('sales_details', 'sls_ord_num, sls_prd_key')
        ) AS t(table_name, key_columns)
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*) FROM (
                SELECT ' || key_cursor.key_columns || '
                FROM datawarehouse.silver.`' || key_cursor.table_name || '`
                GROUP BY ' || key_cursor.key_columns || '
                HAVING COUNT(*) > 1
             )'
        INTO duplicate_count;

        IF duplicate_count > 0 THEN
            SELECT
                key_cursor.table_name  AS table_name,
                key_cursor.key_columns AS key_columns,
                duplicate_count        AS duplicate_key_groups;
        END IF;

    END FOR;

END;