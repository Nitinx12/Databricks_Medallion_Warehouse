BEGIN

    DECLARE row_count BIGINT;
    DECLARE null_count BIGINT;
    DECLARE duplicate_count BIGINT;

    FOR table_cursor AS
        SELECT table_name
        FROM datawarehouse.information_schema.tables
        WHERE table_schema = 'bronze'
          AND table_type = 'BASE TABLE'
    DO

        -- Check NULL updated_at values
        EXECUTE IMMEDIATE
            'SELECT COUNT(*)
             FROM datawarehouse.bronze.`'
             || table_cursor.table_name
             || '`
             WHERE updated_at IS NULL'
        INTO null_count;


        -- Check duplicate updated_at values
        EXECUTE IMMEDIATE
            'SELECT COUNT(*)
             FROM (
                 SELECT updated_at
                 FROM datawarehouse.bronze.`'
                 || table_cursor.table_name
                 || '`
                 GROUP BY updated_at
                 HAVING COUNT(*) > 1
             )'
        INTO duplicate_count;


        -- Test result
        SELECT
            table_cursor.table_name AS table_name,
            null_count,
            duplicate_count,
            CASE
                WHEN null_count = 0
                 AND duplicate_count = 0
                THEN 'PASS'
                ELSE 'FAIL'
            END AS test_result;

    END FOR;

END;