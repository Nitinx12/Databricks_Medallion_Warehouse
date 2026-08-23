-- Loops the FK columns on fct_sales and flags rows where the LEFT JOIN to
-- the dimension never matched (NULL surrogate key). Especially relevant
-- given fct_sales joins on CAST(CAST(x AS DOUBLE) AS BIGINT) for
-- customer_id -- a fragile type-normalization pattern that can silently
-- fail to match. Returns empty when clean.

BEGIN

    DECLARE unmatched_count BIGINT;

    FOR fk_cursor AS
        SELECT * FROM (VALUES
            ('fct_sales', 'customer_key'),
            ('fct_sales', 'product_key')
        ) AS t(fact_table, fk_column)
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*)
             FROM datawarehouse.gold.`' || fk_cursor.fact_table || '`
             WHERE `' || fk_cursor.fk_column || '` IS NULL'
        INTO unmatched_count;

        IF unmatched_count > 0 THEN
            SELECT
                fk_cursor.fact_table AS fact_table,
                fk_cursor.fk_column  AS fk_column,
                unmatched_count      AS unmatched_rows;
        END IF;

    END FOR;

END;