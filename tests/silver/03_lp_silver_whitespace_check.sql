-- Loops every STRING column in silver and flags leading/trailing whitespace
-- that should have been stripped by TRIM() in the model. Catches cases
-- where a new source column was added but never wrapped in TRIM(). Returns
-- empty when clean.

BEGIN

    DECLARE untrimmed_count BIGINT;

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
             WHERE `'
             || column_cursor.column_name
             || '` IS NOT NULL
               AND `'
             || column_cursor.column_name
             || '` <> TRIM(`'
             || column_cursor.column_name
             || '`)'
        INTO untrimmed_count;

        IF untrimmed_count > 0 THEN
            SELECT
                column_cursor.table_name  AS table_name,
                column_cursor.column_name AS column_name,
                untrimmed_count           AS untrimmed_rows;
        END IF;

    END FOR;

END;