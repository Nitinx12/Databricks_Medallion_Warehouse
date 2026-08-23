-- Config-driven loop: for each (table, column, allowed_values) row, flags
-- any value outside the accepted set. Passes the allowed values in as a
-- bind parameter via USING/array_contains rather than string-concatenating
-- quoted literals, so there's no quote-escaping risk. Catches a new raw
-- code (e.g. a gender or marital-status code the CASE logic doesn't map)
-- silently falling through as an unexpected value instead of 'N/A'.
-- Returns empty when clean.

BEGIN

    DECLARE bad_count BIGINT;

    FOR cat_cursor AS
        SELECT * FROM (VALUES
            ('cust_info', 'cst_marital_status', array('Single', 'Married', 'N/A')),
            ('cust_info', 'cst_gndr',            array('Female', 'Male', 'N/A')),
            ('cust_az12', 'gen',                 array('Female', 'Male', 'N/A')),
            ('prd_info',  'prd_line',            array('Mountain', 'Road', 'Other Sales', 'Touring', 'N/A'))
        ) AS t(table_name, column_name, allowed_values)
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*)
             FROM datawarehouse.silver.`' || cat_cursor.table_name || '`
             WHERE NOT array_contains(:vals, `' || cat_cursor.column_name || '`)'
        INTO bad_count
        USING cat_cursor.allowed_values AS vals;

        IF bad_count > 0 THEN
            SELECT
                cat_cursor.table_name  AS table_name,
                cat_cursor.column_name AS column_name,
                bad_count              AS unexpected_value_rows;
        END IF;

    END FOR;

END;