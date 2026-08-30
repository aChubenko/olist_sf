"""Unit tests for atomic Snowflake Bronze publication."""

from pathlib import Path

import pytest

from scripts.load_bronze import (
    PreparedDataset,
    SnowflakeConfig,
    mark_run_failed,
    prepare_dataset,
    publish_datasets,
)
from scripts.olist_schema import OlistDataset


class RecordingCursor:
    def __init__(
        self,
        *,
        row_count: int = 0,
        fail_on: str | None = None,
    ) -> None:
        self.row_count = row_count
        self.fail_on = fail_on
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []

    def execute(self, statement: str, params=None) -> None:
        normalized = " ".join(statement.split())
        self.calls.append((normalized, params))
        if self.fail_on and self.fail_on in normalized:
            raise RuntimeError("simulated Snowflake failure")

    def fetchone(self) -> tuple[int]:
        return (self.row_count,)


def snowflake_config() -> SnowflakeConfig:
    return SnowflakeConfig(
        account="ci_account",
        user="ci_user",
        password="ci_password",
        role="CI_ROLE",
        warehouse="CI_WH",
        database="OLIST_DWH",
        bronze_schema="BRONZE",
        silver_schema="SILVER",
        gold_schema="GOLD",
        create_resources=False,
        warehouse_size="XSMALL",
    )


def prepared_dataset(table: str, rows: int) -> PreparedDataset:
    dataset = OlistDataset(f"{table.lower()}.csv", table, ("id",))
    return PreparedDataset(
        dataset=dataset,
        target_table=f"OLIST_DWH.BRONZE.{table}",
        shadow_table=f"OLIST_DWH.BRONZE.{table}__LOAD_TEST",
        loaded_rows=rows,
    )


def statements(cursor: RecordingCursor) -> list[str]:
    return [statement for statement, _ in cursor.calls]


def test_prepare_dataset_loads_shadow_table_without_touching_target() -> None:
    cursor = RecordingCursor(row_count=3)
    dataset = OlistDataset("orders.csv", "RAW_ORDERS", ("order_id",))

    result = prepare_dataset(
        cursor,
        snowflake_config(),
        dataset,
        Path("orders.csv"),
        "00000000-0000-0000-0000-000000000001",
        expected_rows=3,
    )

    executed = statements(cursor)
    assert result.loaded_rows == 3
    assert result.shadow_table.endswith(
        "RAW_ORDERS__LOAD_00000000_0000_0000_0000_000000000001"
    )
    assert any(f"COPY INTO {result.shadow_table}" in sql for sql in executed)
    assert not any("TRUNCATE TABLE" in sql for sql in executed)
    assert not any("INSERT OVERWRITE" in sql for sql in executed)
    assert executed[-1].startswith("REMOVE @OLIST_DWH.BRONZE.OLIST_CSV_STAGE/")


def test_prepare_dataset_rejects_row_count_mismatch() -> None:
    cursor = RecordingCursor(row_count=2)
    dataset = OlistDataset("orders.csv", "RAW_ORDERS", ("order_id",))

    with pytest.raises(ValueError, match="expected 3, loaded 2"):
        prepare_dataset(
            cursor,
            snowflake_config(),
            dataset,
            Path("orders.csv"),
            "00000000-0000-0000-0000-000000000001",
            expected_rows=3,
        )

    assert statements(cursor)[-1].startswith(
        "REMOVE @OLIST_DWH.BRONZE.OLIST_CSV_STAGE/"
    )


def test_publish_datasets_commits_one_explicit_transaction() -> None:
    cursor = RecordingCursor()
    prepared = [
        prepared_dataset("RAW_CUSTOMERS", 10),
        prepared_dataset("RAW_ORDERS", 20),
    ]

    publish_datasets(cursor, snowflake_config(), "load-1", prepared)

    executed = statements(cursor)
    assert executed[0] == "BEGIN TRANSACTION"
    assert executed[-1] == "COMMIT"
    assert "ROLLBACK" not in executed
    assert sum(sql.startswith("INSERT OVERWRITE INTO") for sql in executed) == 2
    assert not any("TRUNCATE TABLE" in sql for sql in executed)
    assert any("SET status = 'SUCCESS'" in sql for sql in executed)


def test_publish_datasets_rolls_back_the_whole_refresh_on_failure() -> None:
    cursor = RecordingCursor(fail_on="INSERT OVERWRITE INTO OLIST_DWH.BRONZE.RAW_ORDERS")
    prepared = [
        prepared_dataset("RAW_CUSTOMERS", 10),
        prepared_dataset("RAW_ORDERS", 20),
    ]

    with pytest.raises(RuntimeError, match="simulated Snowflake failure"):
        publish_datasets(cursor, snowflake_config(), "load-1", prepared)

    executed = statements(cursor)
    assert executed[0] == "BEGIN TRANSACTION"
    assert executed[-1] == "ROLLBACK"
    assert "COMMIT" not in executed


def test_failed_run_audit_records_progress_and_error() -> None:
    cursor = RecordingCursor()
    prepared = [prepared_dataset("RAW_CUSTOMERS", 10)]

    mark_run_failed(
        cursor,
        snowflake_config(),
        "load-1",
        prepared,
        RuntimeError("copy failed"),
    )

    statement, params = cursor.calls[-1]
    assert "SET status = 'FAILED'" in statement
    assert params == (1, 10, "RuntimeError: copy failed", "load-1")
