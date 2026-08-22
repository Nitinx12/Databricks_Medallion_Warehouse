"""
Connection functions for PostgreSQL, MongoDB, and Databricks.
Built on top of the env config validated in engine.py.
"""

from contextlib import contextmanager

from databricks import sql as databricks_sql
from pymongo import MongoClient
from pymongo.database import Database
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from engine import (
    DATABRICKS_HOST,
    DATABRICKS_HTTP_PATH,
    DATABRICKS_TOKEN,
    MONGO_DB,
    MONGO_URL,
    POSTGRES_DATABASE,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USERNAME,
)


# =========================================================
# POSTGRES
# =========================================================

def get_postgres_engine(echo: bool = False) -> Engine:
    """Create and return a SQLAlchemy engine for PostgreSQL."""
    connection_url = (
        f"postgresql+psycopg2://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
    )
    try:
        engine = create_engine(
            connection_url,
            echo=echo,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
        with engine.connect():
            pass
        print(f"Connected to PostgreSQL ({POSTGRES_DATABASE} @ {POSTGRES_HOST}:{POSTGRES_PORT})")
        return engine
    except Exception as error:
        print(f"PostgreSQL connection failed: {error}")
        raise


@contextmanager
def get_postgres_connection(echo: bool = False) -> Connection:
    """Yield a Postgres connection, disposing the engine on exit."""
    engine = get_postgres_engine(echo=echo)
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()
        engine.dispose()


# =========================================================
# MONGO
# =========================================================

def get_mongo_client() -> MongoClient:
    """Create and return a MongoDB client, verified with a ping."""
    try:
        client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print(f"Connected to MongoDB ({MONGO_URL})")
        return client
    except Exception as error:
        print(f"MongoDB connection failed: {error}")
        raise


def get_mongo_database() -> Database:
    """Return the target Mongo database (opens a new client each call)."""
    client = get_mongo_client()
    return client[MONGO_DB]


# =========================================================
# DATABRICKS
# =========================================================

@contextmanager
def get_databricks_connection():
    """Yield a Databricks SQL connection, closing it on exit."""
    try:
        connection = databricks_sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        )
        print(f"Connected to Databricks ({DATABRICKS_HOST})")
    except Exception as error:
        print(f"Databricks connection failed: {error}")
        raise

    try:
        yield connection
    finally:
        connection.close()


# =========================================================
# MANUAL TEST
# =========================================================

if __name__ == "__main__":
    print("=== Connection Check ===")

    get_postgres_engine()
    get_mongo_client()
    with get_databricks_connection():
        pass