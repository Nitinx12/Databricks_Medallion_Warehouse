-- Loops every numeric column in silver whose name suggests it should never
-- be negative (cost, price, quantity, sales, amount) and flags negative
-- values. Guards against the sls_price/sls_sales resolution logic in
-- sales_details regressing and letting a negative slip through instead of
-- being ABS()'d. Returns empty when clean.

BEGIN

    DECLARE negative_count BIGINT;

    FOR column_cursor AS
        SELECT
            table_name,
            column_name
        FROM datawarehouse.information_schema.columns
        WHERE table_schema = 'silver'
          AND (
                data_type IN ('INT', 'BIGINT', 'DOUBLE', 'FLOAT')
                OR data_type LIKE 'DECIMAL%'
              )
          AND (
                column_name LIKE '%cost%'
                OR column_name LIKE '%price%'
                OR column_name LIKE '%quantity%'
                OR column_name LIKE '%sales%'
                OR column_name LIKE '%amount%'
              )
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*)
             FROM datawarehouse.silver.`'
             || column_cursor.table_name
             || '`
             WHERE `'
             || column_cursor.column_name
             || '` < 0'
        INTO negative_count;

        IF negative_count > 0 THEN
            SELECT
                column_cursor.table_name  AS table_name,
                column_cursor.column_name AS column_name,
                negative_count             AS negative_value_rows;
        END IF;

    END FOR;

END;