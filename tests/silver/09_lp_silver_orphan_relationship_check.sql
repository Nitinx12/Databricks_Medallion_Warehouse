-- Config-driven loop: generalizes the manual no_orphan_rows dbt tests into
-- one script covering every known FK relationship in the silver layer.
-- For each (child_table, child_column, parent_table, parent_column,
-- extra_filter) row, counts child rows whose value has no match in the
-- parent. extra_filter is an optional raw SQL condition (ANDed onto the
-- child side) for excluding known, accepted gaps - kept in sync with the
-- `where` clauses on the matching no_orphan_rows/relationships tests in
-- schema.yml so this script never re-flags something already documented
-- and accepted there. Returns empty when clean.
--
-- NOTE: sales_details.sls_cust_id -> cust_info.cst_id is intentionally NOT
-- checked here. schema.yml documents that the two systems use different
-- key formats/domains for customer identity - fct_sales.sql even has to
-- CAST both sides through DOUBLE -> BIGINT to match them up. A plain
-- string-equality orphan check against that pair produces false positives,
-- not real orphans.

BEGIN

    DECLARE orphan_count BIGINT;

    FOR fk_cursor AS
        SELECT * FROM (VALUES
            ('cust_az12',     'cid',         'cust_info',   'cst_key', ''),
            ('loc_a101',      'cid',         'cust_info',   'cst_key', ''),
            -- Known, accepted gap: cat_id = 'CO_PE' has no match in
            -- px_cat_g1v2 for 7 products (see schema.yml, prd_info.cat_id,
            -- severity: warn). Excluded here so it doesn't re-surface, but
            -- any *other* orphaned cat_id still will.
            ('prd_info',      'cat_id',      'px_cat_g1v2', 'id',      "AND c.`cat_id` != 'CO_PE'"),
            ('sales_details', 'sls_prd_key', 'prd_info',    'prd_key', '')
        ) AS t(child_table, child_column, parent_table, parent_column, extra_filter)
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*)
             FROM datawarehouse.silver.`' || fk_cursor.child_table || '` c
             WHERE c.`' || fk_cursor.child_column || '` IS NOT NULL
               ' || fk_cursor.extra_filter || '
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