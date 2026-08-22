from dotenv import load_dotenv
import os

load_dotenv()

# =========================================================
# POSTGRES
# =========================================================
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# =========================================================
# MONGO
# =========================================================
MONGO_URL = os.getenv("MONGO_URL")
MONGO_DB = os.getenv("MONGO_DB")

# =========================================================
# DATABRICKS
# =========================================================
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG")
DATABRICKS_SCHEMA_BRONZE = os.getenv("DATABRICKS_SCHEMA_BRONZE")
DATABRICKS_SCHEMA_SILVER = os.getenv("DATABRICKS_SCHEMA_SILVER")
DATABRICKS_SCHEMA_GOLD = os.getenv("DATABRICKS_SCHEMA_GOLD")


# =========================================================
# VALIDATION
# =========================================================

_required = {
    # Postgres Validation
    "POSTGRES_HOST": POSTGRES_HOST,
    "POSTGRES_PORT": POSTGRES_PORT,
    "POSTGRES_DATABASE": POSTGRES_DATABASE,
    "POSTGRES_USERNAME": POSTGRES_USERNAME,
    "POSTGRES_PASSWORD": POSTGRES_PASSWORD,

    # Mongo Validation
    "MONGO_URL": MONGO_URL,
    "MONGO_DB": MONGO_DB,

    # Databricks Validation
    "DATABRICKS_HOST": DATABRICKS_HOST,
    "DATABRICKS_HTTP_PATH": DATABRICKS_HTTP_PATH,
    "DATABRICKS_TOKEN": DATABRICKS_TOKEN,
    "DATABRICKS_CATALOG": DATABRICKS_CATALOG,
    "DATABRICKS_SCHEMA_BRONZE": DATABRICKS_SCHEMA_BRONZE,
    "DATABRICKS_SCHEMA_SILVER": DATABRICKS_SCHEMA_SILVER,
    "DATABRICKS_SCHEMA_GOLD": DATABRICKS_SCHEMA_GOLD
}

_missing = [k for k, v in _required.items() if not v]

if _missing:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(_missing)}"
    )