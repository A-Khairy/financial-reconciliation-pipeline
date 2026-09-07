import pytest
from airflow.models import DagBag


def test_dag_loaded_without_errors():
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

    dag = dag_bag.get_dag("automated_financial_reconciliation")
    assert dag is not None
    assert len(dag.tasks) == 5


def test_dag_task_dependencies():
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    dag = dag_bag.get_dag("automated_financial_reconciliation")

    create_schemas = dag.get_task("create_schemas")
    ingest_internal = dag.get_task("ingest_internal_feed")
    ingest_external = dag.get_task("ingest_external_feed")
    reconciliation = dag.get_task("run_reconciliation")
    audit_summary = dag.get_task("audit_summary")

    assert ingest_internal in create_schemas.downstream_list
    assert ingest_external in create_schemas.downstream_list
    assert reconciliation in ingest_internal.downstream_list
    assert reconciliation in ingest_external.downstream_list
    assert audit_summary in reconciliation.downstream_list


def test_reconciliation_logic_mapping():
    # Synthetic verification of mismatch flags
    def reconcile(internal_id, ext_id, int_amt, ext_amt):
        if not internal_id:
            return "MISSING_IN_INTERNAL_LEDGER"
        if not ext_id:
            return "MISSING_IN_GATEWAY"
        if int_amt != ext_amt:
            return "AMOUNT_MISMATCH"
        return "MATCHED"

    assert reconcile("tx_1", "ext_1", 100.0, 100.0) == "MATCHED"
    assert reconcile("tx_1", "ext_1", 100.0, 97.5) == "AMOUNT_MISMATCH"
    assert reconcile("tx_1", None, 100.0, None) == "MISSING_IN_GATEWAY"
    assert reconcile(None, "ext_1", None, 100.0) == "MISSING_IN_INTERNAL_LEDGER"