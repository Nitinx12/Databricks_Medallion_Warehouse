"""
Append-only, incremental copy of PostgreSQL and MongoDB data into the
Databricks bronze layer.

This is a "dump-and-load" ingestion engine: it discovers source tables and
collections, pulls only rows/documents changed since the last watermark, and
appends them straight into bronze Delta tables using the same source names
and columns. It does not merge, update, deduplicate, or match against
existing rows - it never inspects target row values, only the target's
MAX(updated_at) watermark.

Each source must contain an `updated_at` field. No stable key (primary key /
`_id`) is required here, because bronze never upserts.

Watermarks use `>=` so rows with the same timestamp are not skipped; this can
occasionally re-append a row unchanged since the last run. That's expected
and safe: the silver dbt models read from these bronze tables and use
`materialized='incremental'` + `incremental_strategy='merge'` on a
unique_key to de-duplicate and upsert into silver. Bronze is intentionally
allowed to contain duplicates - silver is where "one row per key" is
enforced.

Author: Nitin
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
CHUNK_SIZE = 10_000  # rows per JDBC fetch and upper bound on rows per INSERT batch
MAX_INSERT_PARAMETERS = 10_000  # Databricks Thrift server hard limit per parameterized query
MAX_PARALLEL_TABLES = 4  # tune down/up based on your SQL warehouse's concurrent-query capacity
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
    #
    # NOTE: this is a plain local/standalone SparkSession (JDBC + Mongo
    # connector jars only) - it is NOT attached to the Databricks workspace's
    # Unity Catalog. All writes to Databricks go through
    # utils.connection.get_databricks_connection() (databricks-sql-connector
    # over the SQL warehouse), not through this Spark session's writer. If
    # you later move this to run via Databricks Connect / on a Databricks
    # cluster, dataframe.write.saveAsTable(target) becomes an option and can
    # replace insert_rows() below with a single native append.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.appName("BronzeAppendOnlyCopy")
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


def discover_postgres(engine) -> list[dict]:
    """Discover user tables that have an updated_at column to watermark on.

    No key discovery here - bronze is append-only, so there is nothing to
    match rows against.
    """
    table_sql = text("""
        SELECT table_schema, table_name
        FROM information_schema.columns
        WHERE column_name = :incremental_column
          AND table_schema NOT IN ('information_schema', 'pg_catalog')
        GROUP BY table_schema, table_name
        ORDER BY table_schema, table_name
    """)
    # PERF: engine is now created once in main() and passed in, instead of
    # this function opening (and disposing) its own engine every call.
    with engine.connect() as connection:
        tables = connection.execute(table_sql, {"incremental_column": INCREMENTAL_COLUMN}).mappings().all()
        return [
            {"source": "postgres", "schema": row["table_schema"], "name": row["table_name"]}
            for row in tables
        ]


def discover_mongo(client) -> list[dict]:
    """Discover non-system collections containing an updated_at field."""
    # PERF: client is now created once in main() and passed in, instead of
    # this function opening (and closing) its own client every call.
    database = client[MONGO_DB]
    discovered = []
    for name in database.list_collection_names():
        if not name.startswith("system.") and database[name].find_one({INCREMENTAL_COLUMN: {"$exists": True}}, {"_id": 1}):
            discovered.append({"source": "mongo", "name": name})
    return discovered


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


def get_source_row_count(source: dict, postgres_engine, mongo_client) -> int:
    """Return the complete row/document count for the summary table."""
    # PERF: this is called once per table (often from multiple threads at
    # once). It used to open a brand-new engine/client and tear it down
    # immediately after a single query - now it reuses the engine/client
    # created once in main() (both are documented as thread-safe).
    if source["source"] == "postgres":
        with postgres_engine.connect() as connection:
            source_table = postgres_qualified_name(source["schema"], source["name"])
            return int(connection.execute(text(f"SELECT COUNT(*) FROM {source_table}")).scalar_one())
    return int(mongo_client[MONGO_DB][source["name"]].count_documents({}))


def postgres_literal(value) -> str:
    if isinstance(value, datetime):
        rendered = value.isoformat(sep=" ")
    elif isinstance(value, date):
        rendered = value.isoformat()
    else:
        rendered = str(value)
    return "'" + rendered.replace("'", "''") + "'"


def extract_postgres(spark: SparkSession, source: dict, watermark):
    """Plain (unpartitioned) JDBC read of everything at/after the watermark.

    No NTILE/key-ordered partitioning here: that existed purely to make
    chunked MERGE commits resumable in primary-key-safe order. Bronze writes
    are now a handful of plain INSERT batches with no target matching, so
    there's nothing left that depends on read ordering. If a very large
    table needs parallel reads, add back numPartitions/partitionColumn on a
    numeric or date column.
    """
    source_table = postgres_qualified_name(source["schema"], source["name"])
    query = f"SELECT * FROM {source_table}"
    if watermark is not None:
        query = (
            f"SELECT * FROM {source_table} WHERE {quote_postgres_identifier(INCREMENTAL_COLUMN)} "
            f">= {postgres_literal(watermark)}"
        )
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url).option("dbtable", f"({query}) AS source_rows")
        .option("user", POSTGRES_USERNAME).option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver").option("fetchsize", str(CHUNK_SIZE))
        .load()
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
    # PERF: plain scalars (str/int/float/bool/datetime/None/...) are the vast
    # majority of leaf values processed here - this runs per cell, per row,
    # for the whole chunk. Checking that fast path first (and returning
    # immediately) avoids a hasattr()/isinstance() chain against
    # Row/list/dict for every single scalar. Same results, just reordered.
    if isinstance(value, (datetime, date, time, Decimal, bytes, str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "asDict"):
        return {key: normalize_value(item) for key, item in value.asDict(recursive=False).items()}
    if isinstance(value, list):
        return [normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize_value(item) for key, item in value.items()}
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


def compute_insert_batch_size(column_count: int) -> int:
    """Cap INSERT batches so (rows_in_batch * column_count) parameters never
    exceed Databricks' Thrift server limit, regardless of how wide the table is.
    Tables with more columns get proportionally smaller batches.
    """
    if column_count <= 0:
        return CHUNK_SIZE
    return max(1, min(CHUNK_SIZE, MAX_INSERT_PARAMETERS // column_count))


def insert_rows(cursor, target_table: str, records: list[dict]) -> None:
    """Append one chunk as a single multi-row INSERT.

    No target read, no matching, no per-row loop, no update logic - this is
    a pure append. Silver's dbt incremental models own de-duplication and
    upserts from here on.
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
    sql = f"INSERT INTO {target_table} ({column_list}) VALUES {', '.join(value_rows)}"
    cursor.execute(sql, parameters)


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


def process_object(spark, databricks_conn, source: dict, logger, console: Console, progress: Progress,
                    postgres_engine, mongo_client) -> dict:
    target = qualified_name(DATABRICKS_CATALOG, DATABRICKS_SCHEMA_BRONZE, source["name"])
    watermark = get_last_watermark(databricks_conn, target)
    source_rows = get_source_row_count(source, postgres_engine, mongo_client)
    console.print(f"[cyan]Copying[/cyan] {source['source']}:{source['name']} (watermark: {watermark})")
    dataframe = extract_postgres(spark, source, watermark) if source["source"] == "postgres" else extract_mongo(spark, source, watermark)
    ensure_target(databricks_conn, target, dataframe.schema)
    insert_batch_size = compute_insert_batch_size(len(dataframe.schema.fields))
    scanned = 0
    task = progress.add_task(f"Copying {source['name']}", total=None)
    with databricks_conn.cursor() as cursor:
        for records in rows_in_chunks(dataframe, insert_batch_size):
            insert_rows(cursor, target, records)
            scanned += len(records)
            progress.update(task, advance=len(records))
            logger.info("%s: appended=%s", source["name"], scanned)
    progress.update(task, description=f"[green]Done[/green] {source['name']}")
    bronze_rows = get_databricks_row_count(databricks_conn, target)
    return {
        "source": source["source"],
        "table": source["name"],
        "source_rows": source_rows,
        "bronze_rows": bronze_rows,
        "appended": scanned,
        "status": "success" if scanned else "up to date",
    }


def process_source_safely(spark, source: dict, logger, console: Console, progress: Progress,
                           postgres_engine, mongo_client) -> dict:
    """Run one table's copy on its own Databricks connection.

    Connections/cursors from the databricks-sql-connector are not safe to
    share across threads, so each concurrently-processed table gets its own
    connection rather than reusing one shared across the thread pool.
    """
    try:
        with get_databricks_connection() as databricks_conn:
            return process_object(spark, databricks_conn, source, logger, console, progress,
                                   postgres_engine, mongo_client)
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
    table.add_column("Appended", justify="right", style="green")
    table.add_column("Status")
    for result in results:
        table.add_row(
            result.get("source", "-"),
            result["table"],
            f"{result.get('source_rows', 0):,}",
            f"{result.get('bronze_rows', 0):,}",
            f"{result.get('appended', 0):,}",
            result["status"],
        )
    console.print(table)


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover and append-only copy source data into Databricks bronze")
    parser.add_argument("--source", choices=("postgres", "mongo", "both"), default="both")
    args = parser.parse_args()
    console = Console()
    console.print(Panel.fit("[bold cyan]Bronze Append-Only Load[/bold cyan]"))
    check_packages(console)
    spark = build_spark_session(check_jars(console))
    logger = file_only_logger("loading", "bronze_pipeline")
    # PERF: one Postgres engine / Mongo client for the whole run, shared by
    # discovery and by every table's row count (including concurrent ones
    # in the thread pool below) instead of opening+closing a fresh
    # connection pool per table. Both clients are thread-safe.
    postgres_engine = None
    mongo_client = None
    try:
        if args.source in ("postgres", "both"):
            postgres_engine = get_postgres_engine()
        if args.source in ("mongo", "both"):
            mongo_client = get_mongo_client()

        sources = []
        if postgres_engine is not None:
            sources.extend(discover_postgres(postgres_engine))
        if mongo_client is not None:
            sources.extend(discover_mongo(mongo_client))
        validate_names(sources)
        if not sources:
            raise RuntimeError(f"No source objects with an {INCREMENTAL_COLUMN} column were discovered")
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
                    executor.submit(process_source_safely, spark, source, logger, console, progress,
                                     postgres_engine, mongo_client)
                    for source in sources
                ]
                for future in as_completed(futures):
                    results.append(future.result())
        results.sort(key=lambda result: (result.get("source", ""), result["table"]))
        print_summary(console, results)
    finally:
        spark.stop()
        if postgres_engine is not None:
            postgres_engine.dispose()
        if mongo_client is not None:
            mongo_client.close()


if __name__ == "__main__":
    main()