-- Same intent as 03_future_date_check.sql, but scoped to silver instead of
-- bronze. Bronze future dates are expected and get cleaned (e.g. cust_az12
-- nulls future bdate); silver future dates are NOT expected -- if this
-- returns rows, the cleaning logic in the model regressed. Returns empty
-- when clean.

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