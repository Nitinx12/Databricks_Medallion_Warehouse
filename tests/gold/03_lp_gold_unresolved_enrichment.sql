-- Loops columns that are populated via a LEFT JOIN enrichment with no
-- COALESCE fallback and flags NULLs. dim_products.category/subcategory
-- come from px_cat_g1v2 via a plain LEFT JOIN with no default -- this WILL
-- currently return rows for the known CO_PE gap (7 products), confirming
-- the caveat raised earlier: silencing the dbt orphan test does not stop
-- these columns from being NULL in gold. Add a COALESCE(..., 'N/A') in
-- dim_products.sql if you want this to go quiet. Returns empty when clean.

BEGIN

    DECLARE null_count BIGINT;

    FOR enrich_cursor AS
        SELECT * FROM (VALUES
            ('dim_products',  'category'),
            ('dim_products',  'subcategory'),
            ('dim_customers', 'country')
        ) AS t(table_name, column_name)
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*)
             FROM datawarehouse.gold.`' || enrich_cursor.table_name || '`
             WHERE `' || enrich_cursor.column_name || '` IS NULL'
        INTO null_count;

        IF null_count > 0 THEN
            SELECT
                enrich_cursor.table_name  AS table_name,
                enrich_cursor.column_name AS column_name,
                null_count                AS unresolved_rows;
        END IF;

    END FOR;

END;