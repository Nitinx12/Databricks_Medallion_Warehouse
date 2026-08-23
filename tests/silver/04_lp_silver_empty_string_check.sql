-- Checks STRING columns in silver for empty values.
-- Empty strings should be converted to 'N/A' or NULL by model logic.
-- Uses LENGTH() = 0. Returns empty when clean.

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