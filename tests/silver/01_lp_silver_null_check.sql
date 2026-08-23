-- Dynamically checks every silver column for NULLs, except columns that are
-- intentionally nullable by design (derived end dates, optional order dates,
-- unparsed birth dates, maintenance flag). Add to the exclusion list as new
-- intentionally-nullable columns are introduced. Returns empty when clean.

BEGIN

    DECLARE null_count BIGINT;

    FOR column_cursor AS
        SELECT
            table_name,
            column_name
        FROM datawarehouse.information_schema.columns
        WHERE table_schema = 'silver'
          AND column_name NOT IN (
                'prd_end_dt', 'sls_order_dt', 'sls_ship_dt', 'sls_due_dt',
                'bdate', 'maintenance'
              )
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*)
             FROM datawarehouse.silver.`'
             || column_cursor.table_name
             || '`
             WHERE `'
             || column_cursor.column_name
             || '` IS NULL'
        INTO null_count;

        IF null_count > 0 THEN
            SELECT
                column_cursor.table_name  AS table_name,
                column_cursor.column_name AS column_name,
                null_count                AS null_rows;
        END IF;

    END FOR;

END;