BEGIN

  DECLARE row_count BIGINT;

  FOR table_cursor AS
    SELECT table_name
    FROM datawarehouse.information_schema.tables
    WHERE table_schema = 'bronze'
      AND table_type = 'BASE TABLE'
  DO

    EXECUTE IMMEDIATE
      'SELECT COUNT(*) FROM datawarehouse.bronze.`'
      || table_cursor.table_name
      || '`'
    INTO row_count;

    SELECT
      table_cursor.table_name AS table_name,
      row_count,
      CASE
        WHEN row_count > 0 THEN 'PASS'
        ELSE 'FAIL - TABLE EMPTY'
      END AS test_result;

  END FOR;

END;