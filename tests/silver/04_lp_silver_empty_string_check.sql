-- Loops every STRING column in silver and flags empty-string values.
-- These usually should have been mapped to 'N/A' or NULL by the model's
-- CASE logic -- an empty string slipping through means a code path wasn't
-- covered. Uses LENGTH() = 0 rather than comparing to a literal '' to
-- avoid quote-escaping issues inside the dynamically built SQL string.
-- Returns empty when clean.

BEGIN

    DECLARE empty_count BIGINT;

    FOR column_cursor AS
        SELECT
            table_name,
            column_name
        FROM datawarehouse.information_schema.columns
        WHERE table_schema = 'silver'
          AND data_type = 'STRING'
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*)
             FROM datawarehouse.silver.`'
             || column_cursor.table_name
             || '`
             WHERE LENGTH(`'
             || column_cursor.column_name
             || '`) = 0'
        INTO empty_count;

        IF empty_count > 0 THEN
            SELECT
                column_cursor.table_name  AS table_name,
                column_cursor.column_name AS column_name,
                empty_count                AS empty_string_rows;
        END IF;

    END FOR;

END;