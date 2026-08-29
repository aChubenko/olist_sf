"""Basic integrity checks executed by `astro dev pytest`."""

from airflow.models import DagBag


def test_dags_import_without_errors() -> None:
    dag_bag = DagBag(include_examples=False)

    assert not dag_bag.import_errors, f"DAG import errors: {dag_bag.import_errors}"


def test_olist_medallion_dag_is_available() -> None:
    dag_bag = DagBag(include_examples=False)

    assert "olist_medallion" in dag_bag.dags
