-- Loops every gold table and flags any whose most recent load is older
-- than the staleness threshold below, based on gold_loaded_at. Catches a
-- broken gold-layer run even if the upstream silver layer loaded fine.
-- Adjust staleness_hours_threshold to match your load cadence. Returns
-- empty when clean.

BEGIN

    DECLARE hours_since_load DOUBLE;
    DECLARE staleness_hours_threshold DOUBLE DEFAULT 48.0;

    FOR table_cursor AS
        SELECT table_name
        FROM datawarehouse.information_schema.tables
        WHERE table_schema = 'gold'
          AND table_type <> 'VIEW'
    DO

        EXECUTE IMMEDIATE
            'SELECT CAST(
                (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX(gold_loaded_at))) / 3600.0
             AS DOUBLE)
             FROM datawarehouse.gold.`' || table_cursor.table_name || '`'
        INTO hours_since_load;

        IF hours_since_load > staleness_hours_threshold THEN
            SELECT
                table_cursor.table_name    AS table_name,
                ROUND(hours_since_load, 1) AS hours_since_last_load;
        END IF;

    END FOR;

END;