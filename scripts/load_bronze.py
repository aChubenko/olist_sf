"""Full-refresh Olist CSV files into the Snowflake Bronze layer."""

from __future__ import annotations

import argparse
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path

import snowflake.connector

from olist_schema import DATASETS, OlistDataset
from validate_olist import validate_directory


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Required environment variable {name} is empty")
    return value


def identifier(name: str, value: str | None = None) -> str:
    candidate = (value if value is not None else required_env(name)).strip()
    if not IDENTIFIER_RE.fullmatch(candidate):
        raise ValueError(f"{name} must be an unquoted Snowflake identifier, got {candidate!r}")
    return candidate.upper()


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false, got {value!r}")


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    create_resources: bool
    warehouse_size: str

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        warehouse_size = os.getenv("SNOWFLAKE_WAREHOUSE_SIZE", "XSMALL").upper()
        allowed_sizes = {
            "XSMALL",
            "SMALL",
            "MEDIUM",
            "LARGE",
            "XLARGE",
            "XXLARGE",
        }
        if warehouse_size not in allowed_sizes:
            raise ValueError(
                f"SNOWFLAKE_WAREHOUSE_SIZE must be one of {sorted(allowed_sizes)}"
            )

        return cls(
            account=required_env("SNOWFLAKE_ACCOUNT"),
            user=required_env("SNOWFLAKE_USER"),
            password=required_env("SNOWFLAKE_PASSWORD"),
            role=identifier("SNOWFLAKE_ROLE"),
            warehouse=identifier("SNOWFLAKE_WAREHOUSE"),
            database=identifier("SNOWFLAKE_DATABASE"),
            bronze_schema=identifier("SNOWFLAKE_BRONZE_SCHEMA"),
            silver_schema=identifier("SNOWFLAKE_SILVER_SCHEMA"),
            gold_schema=identifier("SNOWFLAKE_GOLD_SCHEMA"),
            create_resources=env_bool("SNOWFLAKE_CREATE_RESOURCES", True),
            warehouse_size=warehouse_size,
        )


def connect(config: SnowflakeConfig):
    return snowflake.connector.connect(
        account=config.account,
        user=config.user,
        password=config.password,
        role=config.role,
        application="olist_medallion_airflow",
        session_parameters={"QUERY_TAG": "olist_medallion_bronze"},
    )


def bootstrap(cursor, config: SnowflakeConfig) -> None:
    if config.create_resources:
        cursor.execute(
            f"""
            CREATE WAREHOUSE IF NOT EXISTS {config.warehouse}
              WAREHOUSE_SIZE = '{config.warehouse_size}'
              AUTO_SUSPEND = 60
              AUTO_RESUME = TRUE
              INITIALLY_SUSPENDED = TRUE
            """
        )
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config.database}")

    cursor.execute(f"USE WAREHOUSE {config.warehouse}")
    cursor.execute(f"USE DATABASE {config.database}")
    for schema in (config.bronze_schema, config.silver_schema, config.gold_schema):
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {config.database}.{schema}")

    cursor.execute(
        f"""
        CREATE FILE FORMAT IF NOT EXISTS
          {config.database}.{config.bronze_schema}.OLIST_CSV_FORMAT
          TYPE = CSV
          FIELD_DELIMITER = ','
          FIELD_OPTIONALLY_ENCLOSED_BY = '"'
          SKIP_HEADER = 1
          EMPTY_FIELD_AS_NULL = TRUE
          NULL_IF = ('', 'NULL', 'null')
          ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
          ENCODING = 'UTF8'
        """
    )
    cursor.execute(
        f"""
        CREATE STAGE IF NOT EXISTS
          {config.database}.{config.bronze_schema}.OLIST_CSV_STAGE
          FILE_FORMAT = {config.database}.{config.bronze_schema}.OLIST_CSV_FORMAT
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {config.database}.{config.bronze_schema}.LOAD_AUDIT (
          load_id VARCHAR NOT NULL,
          source_file VARCHAR NOT NULL,
          target_table VARCHAR NOT NULL,
          loaded_rows NUMBER NOT NULL,
          loaded_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )


def create_raw_table(cursor, config: SnowflakeConfig, dataset: OlistDataset) -> str:
    table_name = f"{config.database}.{config.bronze_schema}.{dataset.table}"
    raw_columns = ",\n".join(f"{identifier('column', column)} VARCHAR" for column in dataset.columns)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          {raw_columns},
          _load_id VARCHAR NOT NULL,
          _source_file VARCHAR NOT NULL,
          _source_row_number NUMBER NOT NULL,
          _loaded_at TIMESTAMP_TZ NOT NULL
        )
        """
    )
    return table_name


def load_dataset(
    cursor,
    config: SnowflakeConfig,
    dataset: OlistDataset,
    file_path: Path,
    load_id: str,
) -> int:
    table_name = create_raw_table(cursor, config, dataset)
    stage = f"@{config.database}.{config.bronze_schema}.OLIST_CSV_STAGE/{dataset.table.lower()}"
    file_uri = file_path.resolve().as_uri()

    cursor.execute(f"REMOVE {stage}")
    cursor.execute(f"PUT {file_uri} {stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    cursor.execute(f"TRUNCATE TABLE {table_name}")

    target_columns = ", ".join(
        [identifier("column", column) for column in dataset.columns]
        + ["_LOAD_ID", "_SOURCE_FILE", "_SOURCE_ROW_NUMBER", "_LOADED_AT"]
    )
    source_columns = ",\n".join(
        [f"${index}::VARCHAR" for index in range(1, len(dataset.columns) + 1)]
        + [
            f"'{load_id}'::VARCHAR",
            "METADATA$FILENAME::VARCHAR",
            "METADATA$FILE_ROW_NUMBER::NUMBER",
            "CURRENT_TIMESTAMP()",
        ]
    )

    cursor.execute(
        f"""
        COPY INTO {table_name} ({target_columns})
        FROM (
          SELECT {source_columns}
          FROM {stage}
        )
        FILE_FORMAT = (
          FORMAT_NAME = '{config.database}.{config.bronze_schema}.OLIST_CSV_FORMAT'
        )
        ON_ERROR = 'ABORT_STATEMENT'
        FORCE = TRUE
        """
    )
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = int(cursor.fetchone()[0])
    cursor.execute(
        f"""
        INSERT INTO {config.database}.{config.bronze_schema}.LOAD_AUDIT
          (load_id, source_file, target_table, loaded_rows)
        SELECT %s, %s, %s, %s
        """,
        (load_id, dataset.filename, dataset.table, row_count),
    )
    return row_count


def run(data_dir: Path) -> None:
    validate_directory(data_dir)
    config = SnowflakeConfig.from_env()
    load_id = str(uuid.uuid4())

    connection = connect(config)
    try:
        with connection.cursor() as cursor:
            bootstrap(cursor, config)
            for dataset in DATASETS:
                row_count = load_dataset(
                    cursor,
                    config,
                    dataset,
                    data_dir / dataset.filename,
                    load_id,
                )
                print(f"Loaded {dataset.filename} -> {dataset.table}: {row_count:,} rows")
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(
            os.getenv(
                "OLIST_DATA_DIR",
                str(Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "data" / "raw"),
            )
        ),
    )
    args = parser.parse_args()
    run(args.data_dir)


if __name__ == "__main__":
    main()
