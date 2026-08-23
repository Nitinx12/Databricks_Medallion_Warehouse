-- Loops the measure columns on fct_sales and flags negative values.
-- sales_details already resolves sls_price/sls_sales through ABS() in the
-- silver model, so this should always be empty -- it exists as a
-- defense-in-depth check in case a future gold-layer change (e.g. a new
-- CTE, a currency conversion) reintroduces a negative value. Returns
-- empty when clean.

BEGIN

    DECLARE negative_count BIGINT;

    FOR measure_cursor AS
        SELECT * FROM (VALUES
            ('fct_sales', 'sales_amount'),
            ('fct_sales', 'quantity'),
            ('fct_sales', 'price')
        ) AS t(table_name, column_name)
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*)
             FROM datawarehouse.gold.`' || measure_cursor.table_name || '`
             WHERE `' || measure_cursor.column_name || '` < 0'
        INTO negative_count;

        IF negative_count > 0 THEN
            SELECT
                measure_cursor.table_name  AS table_name,
                measure_cursor.column_name AS column_name,
                negative_count              AS negative_value_rows;
        END IF;

    END FOR;

END;