-- Loops each gold table's declared unique_key (from the dbt config block)
-- and verifies it's actually unique in the output. Catches the incremental
-- merge producing duplicate grain -- e.g. if a future change to
-- dim_products' current-version filter (prd_end_dt IS NULL) ever let two
-- "current" rows for the same product_id through. Returns empty when
-- clean.

BEGIN

    DECLARE duplicate_count BIGINT;

    FOR key_cursor AS
        SELECT * FROM (VALUES
            ('dim_customers', 'customer_id'),
            ('dim_products',  'product_id'),
            ('fct_sales',     'order_number, product_key')
        ) AS t(table_name, key_columns)
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*) FROM (
                SELECT ' || key_cursor.key_columns || '
                FROM datawarehouse.gold.`' || key_cursor.table_name || '`
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