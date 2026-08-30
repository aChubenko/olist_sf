"""Atomically full-refresh Olist CSV files into the Snowflake Bronze layer."""

from __future__ import annotations

import argparse
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path

import snowflake.connector

try:
    from .olist_schema import DATASETS, OlistDataset
    from .validate_olist import validate_directory
except ImportError:  # Direct execution: python scripts/load_bronze.py
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


@dataclass(frozen=True)
class PreparedDataset:
    dataset: OlistDataset
    target_table: str
    shadow_table: str
    loaded_rows: int


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
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {config.database}.{config.bronze_schema}.LOAD_RUN_AUDIT (
          load_id VARCHAR NOT NULL,
          status VARCHAR NOT NULL,
          expected_files NUMBER NOT NULL,
          loaded_files NUMBER NOT NULL,
          expected_rows NUMBER NOT NULL,
          loaded_rows NUMBER NOT NULL,
          started_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
          completed_at TIMESTAMP_TZ,
          error_message VARCHAR
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


def run_audit_table(config: SnowflakeConfig) -> str:
    return f"{config.database}.{config.bronze_schema}.LOAD_RUN_AUDIT"


def start_run(
    cursor,
    config: SnowflakeConfig,
    load_id: str,
    expected_files: int,
    expected_rows: int,
) -> None:
    cursor.execute(
        f"""
        INSERT INTO {run_audit_table(config)}
          (load_id, status, expected_files, loaded_files, expected_rows, loaded_rows)
        VALUES (%s, 'RUNNING', %s, 0, %s, 0)
        """,
        (load_id, expected_files, expected_rows),
    )


def mark_run_failed(
    cursor,
    config: SnowflakeConfig,
    load_id: str,
    prepared: list[PreparedDataset],
    error: Exception,
) -> None:
    error_message = f"{type(error).__name__}: {error}"[:4000]
    cursor.execute(
        f"""
        UPDATE {run_audit_table(config)}
        SET status = 'FAILED',
            loaded_files = %s,
            loaded_rows = %s,
            completed_at = CURRENT_TIMESTAMP(),
            error_message = %s
        WHERE load_id = %s
        """,
        (
            len(prepared),
            sum(item.loaded_rows for item in prepared),
            error_message,
            load_id,
        ),
    )


def prepare_dataset(
    cursor,
    config: SnowflakeConfig,
    dataset: OlistDataset,
    file_path: Path,
    load_id: str,
    expected_rows: int,
) -> PreparedDataset:
    target_table = create_raw_table(cursor, config, dataset)
    load_suffix = load_id.replace("-", "_").upper()
    shadow_table = f"{target_table}__LOAD_{load_suffix}"
    stage = (
        f"@{config.database}.{config.bronze_schema}.OLIST_CSV_STAGE/"
        f"{load_id}/{dataset.table.lower()}"
    )
    file_uri = file_path.resolve().as_uri()

    cursor.execute(f"CREATE TEMPORARY TABLE {shadow_table} LIKE {target_table}")
    cursor.execute(f"PUT {file_uri} {stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

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

    try:
        cursor.execute(
            f"""
            COPY INTO {shadow_table} ({target_columns})
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
        cursor.execute(f"SELECT COUNT(*) FROM {shadow_table}")
        loaded_rows = int(cursor.fetchone()[0])
        if loaded_rows != expected_rows:
            raise ValueError(
                f"Row-count mismatch for {dataset.filename}: "
                f"expected {expected_rows}, loaded {loaded_rows}"
            )
    finally:
        try:
            cursor.execute(f"REMOVE {stage}")
        except Exception as cleanup_error:
            print(f"Warning: failed to clean stage path {stage}: {cleanup_error}")

    return PreparedDataset(
        dataset=dataset,
        target_table=target_table,
        shadow_table=shadow_table,
        loaded_rows=loaded_rows,
    )


def publish_datasets(
    cursor,
    config: SnowflakeConfig,
    load_id: str,
    prepared: list[PreparedDataset],
) -> None:
    cursor.execute("BEGIN TRANSACTION")
    try:
        for item in prepared:
            cursor.execute(
                f"INSERT OVERWRITE INTO {item.target_table} "
                f"SELECT * FROM {item.shadow_table}"
            )

        for item in prepared:
            cursor.execute(
                f"""
                INSERT INTO {config.database}.{config.bronze_schema}.LOAD_AUDIT
                  (load_id, source_file, target_table, loaded_rows)
                SELECT %s, %s, %s, %s
                """,
                (
                    load_id,
                    item.dataset.filename,
                    item.dataset.table,
                    item.loaded_rows,
                ),
            )

        cursor.execute(
            f"""
            UPDATE {run_audit_table(config)}
            SET status = 'SUCCESS',
                loaded_files = %s,
                loaded_rows = %s,
                completed_at = CURRENT_TIMESTAMP(),
                error_message = NULL
            WHERE load_id = %s
            """,
            (
                len(prepared),
                sum(item.loaded_rows for item in prepared),
                load_id,
            ),
        )
        cursor.execute("COMMIT")
    except Exception:
        cursor.execute("ROLLBACK")
        raise


def cleanup_shadow_tables(cursor, prepared: list[PreparedDataset]) -> None:
    for item in prepared:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {item.shadow_table}")
        except Exception as cleanup_error:
            print(
                f"Warning: failed to drop temporary table {item.shadow_table}: "
                f"{cleanup_error}"
            )


def run(data_dir: Path) -> None:
    expected_row_counts = validate_directory(data_dir)
    config = SnowflakeConfig.from_env()
    load_id = str(uuid.uuid4())
    prepared: list[PreparedDataset] = []

    connection = connect(config)
    try:
        with connection.cursor() as cursor:
            bootstrap(cursor, config)
            start_run(
                cursor,
                config,
                load_id,
                expected_files=len(DATASETS),
                expected_rows=sum(expected_row_counts.values()),
            )
            try:
                for dataset in DATASETS:
                    item = prepare_dataset(
                        cursor,
                        config,
                        dataset,
                        data_dir / dataset.filename,
                        load_id,
                        expected_rows=expected_row_counts[dataset.filename],
                    )
                    prepared.append(item)
                    print(
                        f"Prepared {dataset.filename} -> {item.shadow_table}: "
                        f"{item.loaded_rows:,} rows"
                    )

                publish_datasets(cursor, config, load_id, prepared)
                print(
                    f"Published load {load_id}: {len(prepared)} files, "
                    f"{sum(item.loaded_rows for item in prepared):,} rows"
                )
            except Exception as error:
                try:
                    mark_run_failed(cursor, config, load_id, prepared, error)
                except Exception as audit_error:
                    print(f"Warning: failed to record load failure: {audit_error}")
                raise
            finally:
                cleanup_shadow_tables(cursor, prepared)
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
