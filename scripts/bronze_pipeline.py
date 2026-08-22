"""Incrementally copy PostgreSQL and MongoDB source tables into Databricks bronze.

The pipeline discovers source objects at run time. It creates one Delta table per
source object in the configured bronze schema, with the same source-table name
and source columns only. No audit columns and no staging tables are created.

An object must have an ``updated_at`` column/field and a stable key: a PostgreSQL
primary key or MongoDB ``_id``. Rows at the current watermark are re-read using
``>=`` so records with equal timestamps are not skipped.
"""

import argparse
import importlib
import json
import logging
import os
import sys
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time
from decimal import Decimal
from itertools import combinations
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType,
    DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType,
    StructType, TimestampType,
)
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
JARS_DIR = PROJECT_ROOT / "jars"
sys.path.extend((str(PROJECT_ROOT), str(PROJECT_ROOT / "utils")))

from utils.connection import get_databricks_connection, get_mongo_client, get_postgres_engine  # noqa: E402
from utils.engine import (  # noqa: E402
    DATABRICKS_CATALOG, DATABRICKS_SCHEMA_BRONZE, MONGO_DB, MONGO_URL,
    POSTGRES_DATABASE, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USERNAME,
)
from utils.logger import get_logger  # noqa: E402

INCREMENTAL_COLUMN = "updated_at"
CHUNK_SIZE = 10_000
MAX_MERGE_PARAMETERS = 10_000  # Databricks Thrift server hard limit per parameterized query
MAX_PARALLEL_TABLES = 4  # tune down/up based on your SQL warehouse's concurrent-query capacity
POSTGRES_READ_PARTITIONS = 8
POSTGRES_PARTITION_COLUMN = "__bronze_read_partition"
MAX_INFERRED_KEY_COLUMNS = 3
REQUIRED_PACKAGES = ["pyspark", "pymongo", "psycopg2", "databricks.sql", "rich", "sqlalchemy"]
REQUIRED_JARS = {
    "PostgreSQL JDBC": "postgresql*.jar",
    "Mongo BSON": "bson-[0-9]*.jar",
    "Mongo BSON record codec": "bson-record-codec-*.jar",
    "Mongo Spark Connector": "mongo-spark-connector*.jar",
    "Mongo driver core": "mongodb-driver-core-*.jar",
    "Mongo driver sync": "mongodb-driver-sync-*.jar",
}


def quote_identifier(identifier: str) -> str:
    """Quote an identifier so source casing and special characters are retained."""
    return "`" + identifier.replace("`", "``") + "`"


def qualified_name(*parts: str) -> str:
    return ".".join(quote_identifier(part) for part in parts)


def quote_postgres_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def postgres_qualified_name(*parts: str) -> str:
    return ".".join(quote_postgres_identifier(part) for part in parts)


def check_packages(console: Console) -> None:
    missing = [package for package in REQUIRED_PACKAGES if importlib.util.find_spec(package) is None]
    if missing:
        raise RuntimeError(f"Missing Python packages: {', '.join(missing)}")
    console.print("[green]Python package check passed.[/green]")


def check_jars(console: Console) -> list[str]:
    paths, missing = [], []
    for label, pattern in REQUIRED_JARS.items():
        matches = sorted(JARS_DIR.glob(pattern))
        if matches:
            paths.extend(str(match) for match in matches)
        else:
            missing.append(f"{label} ({pattern})")
    if missing:
        raise RuntimeError(f"Missing JARs in {JARS_DIR}: {', '.join(missing)}")
    console.print("[green]Spark connector JAR check passed.[/green]")
    return paths


def build_spark_session(jar_paths: list[str]) -> SparkSession:
    # Always pin PySpark to the interpreter that is actually executing this
    # script (sys.executable is a fully-resolved absolute path). A relative
    # or stale PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON value (e.g. a leftover
    # ".venv\Scripts\python.exe" while actually running inside a differently
    # named conda env) breaks Spark's Windows SPARK_HOME autodetection and
    # can mismatch the driver/worker Python version. Setting these before
    # the SparkSession is built, and overriding whatever _env supplied,
    # makes this work the same regardless of which environment is active.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.appName("BronzeIncrementalCopy")
        .config("spark.jars", ",".join(jar_paths))
        .config("spark.mongodb.read.connection.uri", MONGO_URL)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        # Multiple tables now submit Spark jobs concurrently from separate
        # threads (see process_source_safely / ThreadPoolExecutor in main).
        # The default FIFO scheduler would queue one table's stages behind
        # another's; FAIR lets them interleave instead.
        .config("spark.scheduler.mode", "FAIR")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def discover_postgres() -> list[dict]:
    """Discover user tables with updated_at and a stable replacement key."""
    table_sql = text("""
        SELECT table_schema, table_name
        FROM information_schema.columns
        WHERE column_name = :incremental_column
          AND table_schema NOT IN ('information_schema', 'pg_catalog')
        GROUP BY table_schema, table_name
        ORDER BY table_schema, table_name
    """)
    key_sql = text("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = :table_schema
          AND tc.table_name = :table_name
        ORDER BY kcu.ordinal_position
    """)
    engine = get_postgres_engine()
    try:
        with engine.connect() as connection:
            tables = connection.execute(table_sql, {"incremental_column": INCREMENTAL_COLUMN}).mappings().all()
            discovered = []
            for row in tables:
                primary_key = list(connection.execute(key_sql, row).scalars())
                if not primary_key:
                    primary_key = infer_postgres_key(connection, row["table_schema"], row["table_name"])
                if primary_key:
                    discovered.append({"source": "postgres", "schema": row["table_schema"], "name": row["table_name"], "primary_key": primary_key})
            return discovered
    finally:
        engine.dispose()


def infer_postgres_key(connection, table_schema: str, table_name: str) -> list[str]:
    """Find a non-null unique key when source DDL did not declare one.

    This is intentionally capped at three columns. A table without a proven key
    is excluded rather than risking updates being matched to the wrong rows.
    """
    columns_sql = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :table_schema AND table_name = :table_name
          AND column_name <> :incremental_column
        ORDER BY ordinal_position
    """)
    columns = list(connection.execute(columns_sql, {
        "table_schema": table_schema,
        "table_name": table_name,
        "incremental_column": INCREMENTAL_COLUMN,
    }).scalars())
    source_table = postgres_qualified_name(table_schema, table_name)
    for width in range(1, min(MAX_INFERRED_KEY_COLUMNS, len(columns)) + 1):
        for candidate in combinations(columns, width):
            selected = ", ".join(quote_postgres_identifier(column) for column in candidate)
            null_predicate = " OR ".join(f"{quote_postgres_identifier(column)} IS NULL" for column in candidate)
            profile_sql = text(
                f"SELECT COUNT(*) AS row_count, "
                f"COUNT(DISTINCT ({selected})) AS distinct_count, "
                f"COUNT(*) FILTER (WHERE {null_predicate}) AS null_count "
                f"FROM {source_table}"
            )
            row = connection.execute(profile_sql).mappings().one()
            if row["row_count"] and row["null_count"] == 0 and row["row_count"] == row["distinct_count"]:
                return list(candidate)
    return []


def discover_mongo() -> list[dict]:
    """Discover non-system collections containing an updated_at field."""
    client = get_mongo_client()
    try:
        database = client[MONGO_DB]
        discovered = []
        for name in database.list_collection_names():
            if not name.startswith("system.") and database[name].find_one({INCREMENTAL_COLUMN: {"$exists": True}}, {"_id": 1}):
                discovered.append({"source": "mongo", "name": name, "primary_key": ["_id"]})
        return discovered
    finally:
        client.close()


def validate_names(objects: list[dict]) -> None:
    names = [item["name"] for item in objects]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise RuntimeError("Exact-name bronze tables collide across sources: " + ", ".join(duplicates))


def get_last_watermark(databricks_conn, target_table: str):
    try:
        with databricks_conn.cursor() as cursor:
            cursor.execute(f"SELECT MAX({quote_identifier(INCREMENTAL_COLUMN)}) FROM {target_table}")
            row = cursor.fetchone()
            return row[0] if row and row[0] is not None else None
    except Exception:
        return None


def get_databricks_row_count(databricks_conn, target_table: str) -> int:
    with databricks_conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def get_source_row_count(source: dict) -> int:
    """Return the complete row/document count for the summary table."""
    if source["source"] == "postgres":
        engine = get_postgres_engine()
        try:
            with engine.connect() as connection:
                source_table = postgres_qualified_name(source["schema"], source["name"])
                return int(connection.execute(text(f"SELECT COUNT(*) FROM {source_table}")).scalar_one())
        finally:
            engine.dispose()
    client = get_mongo_client()
    try:
        return int(client[MONGO_DB][source["name"]].count_documents({}))
    finally:
        client.close()


def postgres_literal(value) -> str:
    if isinstance(value, datetime):
        rendered = value.isoformat(sep=" ")
    elif isinstance(value, date):
        rendered = value.isoformat()
    else:
        rendered = str(value)
    return "'" + rendered.replace("'", "''") + "'"


def extract_postgres(spark: SparkSession, source: dict, watermark):
    source_table = postgres_qualified_name(source["schema"], source["name"])
    source_query = f"SELECT * FROM {source_table}"
    if watermark is not None:
        source_query = (
            f"SELECT * FROM {source_table} WHERE {quote_postgres_identifier(INCREMENTAL_COLUMN)} "
            f">= {postgres_literal(watermark)}"
        )
    order_by = ", ".join(quote_postgres_identifier(column) for column in source["primary_key"])
    query = f"""(
        SELECT source_rows.*, NTILE({POSTGRES_READ_PARTITIONS}) OVER (ORDER BY {order_by}) - 1
               AS {quote_postgres_identifier(POSTGRES_PARTITION_COLUMN)}
        FROM ({source_query}) AS source_rows
    ) AS partitioned_source"""
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url).option("dbtable", query)
        .option("user", POSTGRES_USERNAME).option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver").option("fetchsize", str(CHUNK_SIZE))
        .option("partitionColumn", POSTGRES_PARTITION_COLUMN).option("lowerBound", "0")
        .option("upperBound", str(POSTGRES_READ_PARTITIONS)).option("numPartitions", str(POSTGRES_READ_PARTITIONS))
        .load().drop(POSTGRES_PARTITION_COLUMN)
    )


def mongo_extended_date(value) -> str:
    if isinstance(value, datetime):
        return value.isoformat(timespec="microseconds").replace("+00:00", "Z")
    return str(value)


def extract_mongo(spark: SparkSession, source: dict, watermark):
    reader = spark.read.format("mongodb").option("database", MONGO_DB).option("collection", source["name"])
    if watermark is not None:
        pipeline = [{"$match": {INCREMENTAL_COLUMN: {"$gte": {"$date": mongo_extended_date(watermark)}}}}]
        reader = reader.option("pipeline", json.dumps(pipeline))
    return reader.load()


def spark_type_to_databricks(data_type: DataType) -> str:
    """Translate the complete Spark source schema; complex values remain complex."""
    scalar_types = {
        StringType: "STRING", ByteType: "TINYINT", ShortType: "SMALLINT", IntegerType: "INT",
        LongType: "BIGINT", FloatType: "FLOAT", DoubleType: "DOUBLE", BooleanType: "BOOLEAN",
        DateType: "DATE", TimestampType: "TIMESTAMP", BinaryType: "BINARY",
    }
    for spark_type, sql_type in scalar_types.items():
        if isinstance(data_type, spark_type):
            return sql_type
    if isinstance(data_type, DecimalType):
        return f"DECIMAL({data_type.precision},{data_type.scale})"
    if isinstance(data_type, ArrayType):
        return f"ARRAY<{spark_type_to_databricks(data_type.elementType)}>"
    if isinstance(data_type, MapType):
        return f"MAP<{spark_type_to_databricks(data_type.keyType)},{spark_type_to_databricks(data_type.valueType)}>"
    if isinstance(data_type, StructType):
        fields = ", ".join(f"{quote_identifier(field.name)}: {spark_type_to_databricks(field.dataType)}" for field in data_type.fields)
        return f"STRUCT<{fields}>"
    raise TypeError(f"Unsupported source type {data_type.simpleString()}; refusing to coerce it")


def ensure_target(databricks_conn, target_table: str, schema) -> None:
    columns = ", ".join(f"{quote_identifier(field.name)} {spark_type_to_databricks(field.dataType)}" for field in schema.fields)
    with databricks_conn.cursor() as cursor:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {target_table} ({columns}) USING DELTA")


def normalize_value(value):
    """Convert Spark Row/numpy values while retaining source structure and types."""
    if hasattr(value, "asDict"):
        return {key: normalize_value(item) for key, item in value.asDict(recursive=False).items()}
    if isinstance(value, list):
        return [normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize_value(item) for key, item in value.items()}
    if isinstance(value, (datetime, date, time, Decimal, bytes, str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "item"):
        return value.item()
    return value


def rows_in_chunks(dataframe, chunk_size: int) -> Iterator[list[dict]]:
    batch = []
    for row in dataframe.toLocalIterator(prefetchPartitions=True):
        batch.append({key: normalize_value(value) for key, value in row.asDict(recursive=False).items()})
        if len(batch) == chunk_size:
            yield batch
            batch = []
    if batch:
        yield batch


def compute_merge_batch_size(column_count: int) -> int:
    """Cap MERGE batches so (rows_in_batch * column_count) parameters never
    exceed Databricks' Thrift server limit, regardless of how wide the table is.
    Tables with more columns get proportionally smaller batches.
    """
    if column_count <= 0:
        return CHUNK_SIZE
    return max(1, min(CHUNK_SIZE, MAX_MERGE_PARAMETERS // column_count))


def merge_rows(cursor, target_table: str, primary_key: list[str], records: list[dict]) -> None:
    """Atomically upsert one chunk without creating a staging table.

    The source relation is built as a single VALUES(...) table constructor
    rather than a UNION ALL of one SELECT per row. Functionally identical,
    but Databricks' SQL analyzer only has to plan one relation instead of
    reconciling the schema of N separately-planned SELECT branches, which is
    what made batches of even a few thousand rows slow to compile.
    """
    columns = list(records[0])
    column_list = ", ".join(quote_identifier(column) for column in columns)
    value_rows = []
    parameters = {}
    for row_index, record in enumerate(records):
        markers = []
        for column_index, column in enumerate(columns):
            marker = f"value_{row_index}_{column_index}"
            markers.append(f":{marker}")
            parameters[marker] = record[column]
        value_rows.append("(" + ", ".join(markers) + ")")
    join_condition = " AND ".join(
        f"target.{quote_identifier(column)} = source.{quote_identifier(column)}" for column in primary_key
    )
    rows_match = " AND ".join(
        f"target.{quote_identifier(column)} <=> source.{quote_identifier(column)}" for column in columns
    )
    sql = f"""
        MERGE INTO {target_table} AS target
        USING (
            SELECT * FROM VALUES {', '.join(value_rows)} AS source({column_list})
        ) AS source
        ON {join_condition}
        WHEN MATCHED AND NOT ({rows_match}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    cursor.execute(sql, parameters)


def coerce_metrics_to_dict(metrics) -> dict:
    """Normalize a DESCRIBE HISTORY operationMetrics value into a plain dict.

    The databricks-sql-connector does not deserialize MAP<STRING,STRING>
    columns into a Python dict; depending on driver version it can hand back
    a JSON string, or a list of ("key", "value") tuples / {"key":..,
    "value":..} dicts. Calling .get() directly on that list is what produces
    "'list' object has no attribute 'get'".
    """
    if isinstance(metrics, str):
        return json.loads(metrics)
    if isinstance(metrics, dict):
        return metrics
    if isinstance(metrics, list):
        result = {}
        for item in metrics:
            if isinstance(item, dict) and "key" in item:
                result[item["key"]] = item.get("value")
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                result[item[0]] = item[1]
        return result
    return {}


def latest_merge_metrics(cursor, target_table: str) -> tuple[int, int]:
    """Read inserted and updated row counts from the most recent Delta MERGE."""
    cursor.execute(f"DESCRIBE HISTORY {target_table} LIMIT 1")
    row = cursor.fetchone()
    columns = [description[0] for description in cursor.description]
    if not row:
        return 0, 0
    history = dict(zip(columns, row))
    raw_metrics = history.get("operationMetrics") or history.get("operationmetrics") or {}
    metrics = coerce_metrics_to_dict(raw_metrics)
    inserted = int(metrics.get("numTargetRowsInserted", 0))
    updated = int(metrics.get("numTargetRowsUpdated", 0))
    return inserted, updated


def short_error(error: Exception, limit: int = 240) -> str:
    """Keep generated SQL and row values out of console output and logs."""
    message = " ".join(str(error).split())
    return message[:limit] + ("..." if len(message) > limit else "")


def file_only_logger(stage: str, name: str):
    """Keep detailed logs on disk without competing with Rich's live output."""
    logger = get_logger(stage, name)
    for handler in list(logger.handlers):
        if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
            logger.removeHandler(handler)
    return logger


def process_object(spark, databricks_conn, source: dict, logger, console: Console, progress: Progress) -> dict:
    target = qualified_name(DATABRICKS_CATALOG, DATABRICKS_SCHEMA_BRONZE, source["name"])
    watermark = get_last_watermark(databricks_conn, target)
    source_rows = get_source_row_count(source)
    console.print(f"[cyan]Copying[/cyan] {source['source']}:{source['name']} (watermark: {watermark})")
    dataframe = extract_postgres(spark, source, watermark) if source["source"] == "postgres" else extract_mongo(spark, source, watermark)
    # Process rows in updated_at order (not primary-key order) so that if a
    # later chunk fails mid-table, everything already committed genuinely
    # represents "safe up to here." Otherwise the next run's MAX(updated_at)
    # watermark could land past rows that were never actually written,
    # silently skipping them forever.
    dataframe = dataframe.orderBy(INCREMENTAL_COLUMN)
    ensure_target(databricks_conn, target, dataframe.schema)
    merge_batch_size = compute_merge_batch_size(len(dataframe.schema.fields))
    scanned = inserted = updated = skipped = 0
    task = progress.add_task(f"Copying {source['name']}", total=None)
    with databricks_conn.cursor() as cursor:
        for records in rows_in_chunks(dataframe, merge_batch_size):
            merge_rows(cursor, target, source["primary_key"], records)
            batch_inserted, batch_updated = latest_merge_metrics(cursor, target)
            batch_skipped = len(records) - batch_inserted - batch_updated
            scanned += len(records)
            inserted += batch_inserted
            updated += batch_updated
            skipped += batch_skipped
            progress.update(task, advance=len(records))
            logger.info(
                "%s: processed=%s inserted=%s updated=%s skipped=%s",
                source["name"], scanned, inserted, updated, skipped,
            )
    progress.update(task, description=f"[green]Done[/green] {source['name']}")
    bronze_rows = get_databricks_row_count(databricks_conn, target)
    return {
        "source": source["source"],
        "table": source["name"],
        "source_rows": source_rows,
        "bronze_rows": bronze_rows,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "status": "success" if scanned else "up to date",
    }


def process_source_safely(spark, source: dict, logger, console: Console, progress: Progress) -> dict:
    """Run one table's copy on its own Databricks connection.

    Connections/cursors from the databricks-sql-connector are not safe to
    share across threads, so each concurrently-processed table gets its own
    connection rather than reusing one shared across the thread pool.
    """
    try:
        with get_databricks_connection() as databricks_conn:
            return process_object(spark, databricks_conn, source, logger, console, progress)
    except Exception as error:
        message = short_error(error)
        logger.error("%s failed: %s", source["name"], message)
        return {"source": source.get("source", "-"), "table": source["name"], "rows": 0, "status": f"error: {message}"}


def print_summary(console: Console, results: list[dict]) -> None:
    table = Table(title="Bronze Load Summary", show_lines=True)
    table.add_column("Source")
    table.add_column("Table")
    table.add_column("Source rows", justify="right")
    table.add_column("Bronze rows", justify="right")
    table.add_column("Inserted", justify="right", style="green")
    table.add_column("Updated", justify="right", style="yellow")
    table.add_column("Skipped", justify="right", style="dim")
    table.add_column("Status")
    for result in results:
        table.add_row(
            result.get("source", "-"),
            result["table"],
            f"{result.get('source_rows', 0):,}",
            f"{result.get('bronze_rows', 0):,}",
            f"{result.get('inserted', 0):,}",
            f"{result.get('updated', 0):,}",
            f"{result.get('skipped', 0):,}",
            result["status"],
        )
    console.print(table)


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover and incrementally copy source data into Databricks bronze")
    parser.add_argument("--source", choices=("postgres", "mongo", "both"), default="both")
    args = parser.parse_args()
    console = Console()
    console.print(Panel.fit("[bold cyan]Bronze Incremental Copy[/bold cyan]"))
    check_packages(console)
    spark = build_spark_session(check_jars(console))
    logger = file_only_logger("loading", "bronze_pipeline")
    try:
        sources = []
        if args.source in ("postgres", "both"):
            sources.extend(discover_postgres())
        if args.source in ("mongo", "both"):
            sources.extend(discover_mongo())
        validate_names(sources)
        if not sources:
            raise RuntimeError(f"No source objects with {INCREMENTAL_COLUMN} and a stable key were discovered")
        results = []
        progress_columns = (
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TextColumn("[cyan]{task.completed:,.0f} rows processed[/cyan]"),
            TimeElapsedColumn(),
        )
        worker_count = min(MAX_PARALLEL_TABLES, len(sources))
        with Progress(*progress_columns, console=console, transient=True) as progress:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [
                    executor.submit(process_source_safely, spark, source, logger, console, progress)
                    for source in sources
                ]
                for future in as_completed(futures):
                    results.append(future.result())
        results.sort(key=lambda result: (result.get("source", ""), result["table"]))
        print_summary(console, results)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()