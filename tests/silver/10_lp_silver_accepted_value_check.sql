-- Config-driven loop: for each (table, column, allowed_values) row, flags
-- any value outside the accepted set. Catches a new raw code (e.g. a
-- gender or marital-status code the CASE logic doesn't map) silently
-- falling through as an unexpected value instead of 'N/A'. Returns empty
-- when clean.
--
-- The allowed values are converted to a quoted, comma-separated literal
-- list and concatenated straight into the dynamic SQL text (same technique
-- as 09_lp_silver_orphan_relationship_check.sql), rather than passed as a
-- named bind parameter via EXECUTE IMMEDIATE ... USING ... AS. That USING
-- form collides with the Databricks SQL connector's own client-side
-- parameter handling when cursor.execute() is called without a
-- `parameters` argument, and throws PARSE_SYNTAX_ERROR near '@'.

BEGIN

    DECLARE bad_count BIGINT;

    FOR cat_cursor AS
        SELECT
            table_name,
            column_name,
            array_join(
                transform(
                    allowed_values,
                    v -> concat(chr(39), replace(v, chr(39), chr(39) || chr(39)), chr(39))
                ),
                ', '
            ) AS allowed_values_sql
        FROM (VALUES
            ('cust_info', 'cst_marital_status', array('Single', 'Married', 'N/A')),
            ('cust_info', 'cst_gndr',            array('Female', 'Male', 'N/A')),
            ('cust_az12', 'gen',                 array('Female', 'Male', 'N/A')),
            ('prd_info',  'prd_line',            array('Mountain', 'Road', 'Other Sales', 'Touring', 'N/A'))
        ) AS t(table_name, column_name, allowed_values)
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*)
             FROM datawarehouse.silver.`' || cat_cursor.table_name || '`
             WHERE `' || cat_cursor.column_name || '` NOT IN (' || cat_cursor.allowed_values_sql || ')'
        INTO bad_count;

        IF bad_count > 0 THEN
            SELECT
                cat_cursor.table_name  AS table_name,
                cat_cursor.column_name AS column_name,
                bad_count              AS unexpected_value_rows;
        END IF;

    END FOR;

END;