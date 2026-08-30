"""Basic integrity checks executed by `astro dev pytest`."""

from airflow.models import DagBag


def test_dags_import_without_errors() -> None:
    dag_bag = DagBag(include_examples=False)

    assert not dag_bag.import_errors, f"DAG import errors: {dag_bag.import_errors}"


def test_olist_medallion_dag_is_available() -> None:
    dag_bag = DagBag(include_examples=False)

    assert "olist_medallion" in dag_bag.dags


def test_cosmos_renders_dbt_models_as_individual_tasks() -> None:
    dag = DagBag(include_examples=False).dags["olist_medallion"]
    task_ids = set(dag.task_ids)

    assert "dbt_transformations.stg_orders.run" in task_ids
    assert "dbt_transformations.fact_orders.run" in task_ids
    assert "dbt_transformations.mart_daily_sales.run" in task_ids
    assert "build_silver" not in task_ids
    assert "build_gold" not in task_ids
