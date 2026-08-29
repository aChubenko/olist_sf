"""Orchestrate the complete Olist Bronze -> Silver -> Gold pipeline."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pendulum
from airflow.sdk import dag, task


AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
PROJECT_ROOT = Path(os.environ.get("OLIST_PROJECT_ROOT", str(AIRFLOW_HOME)))
DBT_PROJECT_DIR = Path(
    os.environ.get("DBT_PROJECT_DIR", str(PROJECT_ROOT / "dbt"))
)
SCRIPT_DIR = Path(os.environ.get("OLIST_SCRIPT_DIR", str(PROJECT_ROOT / "scripts")))
DATA_DIR = Path(
    os.environ.get("OLIST_DATA_DIR", str(PROJECT_ROOT / "data" / "raw"))
)


def run_command(command: list[str], cwd: Path | None = None) -> None:
    printable = " ".join(command)
    print(f"Running: {printable}")
    subprocess.run(command, cwd=cwd, check=True, env=os.environ.copy())


@dag(
    dag_id="olist_medallion",
    description="Olist CSV -> Snowflake Bronze -> dbt Silver -> dbt Gold",
    schedule=os.environ.get("OLIST_DAG_SCHEDULE") or None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 1},
    tags=["olist", "snowflake", "dbt", "medallion"],
)
def olist_medallion():
    @task()
    def download_source() -> None:
        run_command(
            [
                "python",
                str(SCRIPT_DIR / "download_olist.py"),
                "--data-dir",
                str(DATA_DIR),
            ]
        )

    @task()
    def validate_source() -> None:
        run_command(
            [
                "python",
                str(SCRIPT_DIR / "validate_olist.py"),
                "--data-dir",
                str(DATA_DIR),
            ]
        )

    @task()
    def load_bronze() -> None:
        run_command(
            [
                "python",
                str(SCRIPT_DIR / "load_bronze.py"),
                "--data-dir",
                str(DATA_DIR),
            ]
        )

    @task()
    def dbt_debug() -> None:
        run_command(["dbt", "debug", "--project-dir", str(DBT_PROJECT_DIR)])

    @task()
    def source_freshness() -> None:
        run_command(
            ["dbt", "source", "freshness", "--project-dir", str(DBT_PROJECT_DIR)]
        )

    @task()
    def test_bronze() -> None:
        run_command(
            [
                "dbt",
                "test",
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--select",
                "source:bronze.*",
            ]
        )

    @task()
    def build_silver() -> None:
        run_command(
            [
                "dbt",
                "run",
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--select",
                "path:models/silver",
            ]
        )

    @task()
    def test_silver() -> None:
        run_command(
            [
                "dbt",
                "test",
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--select",
                "path:models/silver",
            ]
        )

    @task()
    def build_gold() -> None:
        run_command(
            [
                "dbt",
                "run",
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--select",
                "path:models/gold",
            ]
        )

    @task()
    def test_gold() -> None:
        run_command(
            [
                "dbt",
                "test",
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--select",
                "path:models/gold",
            ]
        )

    downloaded = download_source()
    validated = validate_source()
    bronze = load_bronze()
    debugged = dbt_debug()
    bronze_tested = test_bronze()
    fresh = source_freshness()
    silver = build_silver()
    silver_tested = test_silver()
    gold = build_gold()
    gold_tested = test_gold()

    downloaded >> validated >> bronze >> debugged >> bronze_tested >> fresh
    fresh >> silver >> silver_tested >> gold >> gold_tested


olist_medallion()
