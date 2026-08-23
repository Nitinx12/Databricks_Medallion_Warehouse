-- Config-driven loop: generalizes the manual no_orphan_rows dbt tests into
-- one script covering every known FK relationship in the silver layer.
-- For each (child_table, child_column, parent_table, parent_column) row,
-- counts child rows whose value has no match in the parent. Note: known,
-- accepted gaps (e.g. prd_info.cat_id = 'CO_PE') should be excluded here
-- too if you don't want them re-surfacing in this script. Returns empty
-- when clean.

BEGIN

    DECLARE orphan_count BIGINT;

    FOR fk_cursor AS
        SELECT * FROM (VALUES
            ('cust_az12',     'cid',         'cust_info',   'cst_key'),
            ('loc_a101',      'cid',         'cust_info',   'cst_key'),
            ('prd_info',      'cat_id',      'px_cat_g1v2', 'id'),
            ('sales_details', 'sls_prd_key', 'prd_info',    'prd_key'),
            ('sales_details', 'sls_cust_id', 'cust_info',   'cst_id')
        ) AS t(child_table, child_column, parent_table, parent_column)
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*)
             FROM datawarehouse.silver.`' || fk_cursor.child_table || '` c
             WHERE c.`' || fk_cursor.child_column || '` IS NOT NULL
               AND NOT EXISTS (
                    SELECT 1
                    FROM datawarehouse.silver.`' || fk_cursor.parent_table || '` p
                    WHERE p.`' || fk_cursor.parent_column || '` = c.`' || fk_cursor.child_column || '`
               )'
        INTO orphan_count;

        IF orphan_count > 0 THEN
            SELECT
                fk_cursor.child_table  AS child_table,
                fk_cursor.child_column AS child_column,
                fk_cursor.parent_table AS parent_table,
                orphan_count           AS orphan_rows;
        END IF;

    END FOR;

END;