from pathlib import Path

import polars as pl
import psycopg
from loguru import logger


DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "mydb",
    "user": "admin",
    "password": "admin",
}

DB_URL = "postgresql://admin:admin@localhost:5432/mydb"
DATA_DIR = Path("data")


def create_raw_schema() -> None:
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            logger.info("Creating raw schema if it does not exist")
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")


def drop_dbt_schemas() -> None:
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for schema_name in ("gold", "silver", "bronze"):
                logger.info("Dropping schema {} if it exists", schema_name)
                cur.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")


def load_csv_files() -> None:
    csv_files = sorted(DATA_DIR.glob("*.csv"))

    if not csv_files:
        logger.warning(f"No CSV files found in {DATA_DIR}")
        return

    for file_path in csv_files:
        table_name = file_path.stem
        logger.info(f"Loading {file_path.name} into raw.{table_name}")

        dataframe = pl.read_csv(file_path)
        dataframe.write_database(
            table_name=f"raw.{table_name}",
            connection=DB_URL,
            engine="adbc",
            if_table_exists="replace",
        )

    logger.info(f"Finished loading {len(csv_files)} file(s)")


def main() -> None:
    create_raw_schema()
    drop_dbt_schemas()
    load_csv_files()


if __name__ == "__main__":
    main()
