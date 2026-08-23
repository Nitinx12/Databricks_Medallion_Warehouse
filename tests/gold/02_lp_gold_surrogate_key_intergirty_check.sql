-- Loops each dim table and checks total row count against distinct natural
-- key count and distinct surrogate (xxhash64) key count. A mismatch
-- between total rows and distinct natural key means the model's grain
-- broke (duplicate customer_id/product_id). A mismatch between distinct
-- natural key and distinct surrogate key means an xxhash64 collision.
-- Returns empty when clean.

BEGIN

    DECLARE total_rows         BIGINT;
    DECLARE distinct_natural   BIGINT;
    DECLARE distinct_surrogate BIGINT;

    FOR key_cursor AS
        SELECT * FROM (VALUES
            ('dim_customers', 'customer_id', 'customer_key'),
            ('dim_products',  'product_id',  'product_key')
        ) AS t(table_name, natural_key_column, surrogate_key_column)
    DO

        EXECUTE IMMEDIATE
            'SELECT
                COUNT(*),
                COUNT(DISTINCT `' || key_cursor.natural_key_column || '`),
                COUNT(DISTINCT `' || key_cursor.surrogate_key_column || '`)
             FROM datawarehouse.gold.`' || key_cursor.table_name || '`'
        INTO total_rows, distinct_natural, distinct_surrogate;

        IF total_rows <> distinct_natural OR distinct_natural <> distinct_surrogate THEN
            SELECT
                key_cursor.table_name AS table_name,
                total_rows            AS total_rows,
                distinct_natural      AS distinct_natural_keys,
                distinct_surrogate    AS distinct_surrogate_keys;
        END IF;

    END FOR;

END;