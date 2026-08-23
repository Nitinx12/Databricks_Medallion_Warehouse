BEGIN

    DECLARE future_date_count BIGINT;

    FOR column_cursor AS
        SELECT
            table_name,
            column_name
        FROM datawarehouse.information_schema.columns
        WHERE table_schema = 'bronze'
          AND (
                column_name LIKE '%date%'
                OR column_name LIKE '%updated_at%'
                OR column_name LIKE '%created_at%'
                OR column_name LIKE '%timestamp%'
              )
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*)
             FROM datawarehouse.bronze.`'
             || column_cursor.table_name
             || '`
             WHERE TRY_CAST(`'
             || column_cursor.column_name
             || '` AS TIMESTAMP) > CURRENT_TIMESTAMP()'
        INTO future_date_count;

        IF future_date_count > 0 THEN
            SELECT
                column_cursor.table_name,
                column_cursor.column_name,
                future_date_count AS future_date_rows;
        END IF;

    END FOR;

END;