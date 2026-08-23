-- Checks for unexpected future dates in the silver layer.
-- Bronze future dates may be cleaned during transformation; silver future dates
-- indicate a regression in cleaning logic. Returns empty when clean.

BEGIN

    DECLARE future_date_count BIGINT;

    FOR column_cursor AS
        SELECT
            table_name,
            column_name
        FROM datawarehouse.information_schema.columns
        WHERE table_schema = 'silver'
          AND (
                column_name LIKE '%date%'
                OR column_name LIKE '%_dt'
                OR column_name LIKE '%updated_at%'
                OR column_name LIKE '%loaded_at%'
              )
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*)
             FROM datawarehouse.silver.`'
             || column_cursor.table_name
             || '`
             WHERE TRY_CAST(`'
             || column_cursor.column_name
             || '` AS TIMESTAMP) > CURRENT_TIMESTAMP()'
        INTO future_date_count;

        IF future_date_count > 0 THEN
            SELECT
                column_cursor.table_name  AS table_name,
                column_cursor.column_name AS column_name,
                future_date_count         AS future_date_rows;
        END IF;

    END FOR;

END;