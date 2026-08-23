-- Loops every base table in silver and flags any with zero rows. Catches
-- an incremental merge that accidentally wiped a table, or a full-refresh
-- that ran against an empty source. Returns empty when clean.

BEGIN

    DECLARE row_count BIGINT;

    FOR table_cursor AS
        SELECT table_name
        FROM datawarehouse.information_schema.tables
        WHERE table_schema = 'silver'
          AND table_type <> 'VIEW'
    DO

        EXECUTE IMMEDIATE
            'SELECT COUNT(*) FROM datawarehouse.silver.`' || table_cursor.table_name || '`'
        INTO row_count;

        IF row_count = 0 THEN
            SELECT
                table_cursor.table_name AS table_name,
                row_count               AS row_count;
        END IF;

    END FOR;

END;