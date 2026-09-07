import csv
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "revolut_data_platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

logger = logging.getLogger("airflow.task")


def init_database_schemas():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    schema_sql = """
    CREATE TABLE IF NOT EXISTS internal_transactions (
        transaction_id VARCHAR(64) PRIMARY KEY,
        account_id VARCHAR(64) NOT NULL,
        amount NUMERIC(12, 2) NOT NULL,
        currency VARCHAR(3) NOT NULL,
        status VARCHAR(20) NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS external_gateway_transactions (
        provider_tx_id VARCHAR(64) PRIMARY KEY,
        internal_reference_id VARCHAR(64),
        cleared_amount NUMERIC(12, 2) NOT NULL,
        currency VARCHAR(3) NOT NULL,
        settlement_status VARCHAR(20) NOT NULL,
        cleared_at TIMESTAMP WITH TIME ZONE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS reconciliation_audit (
        audit_id SERIAL PRIMARY KEY,
        batch_date DATE NOT NULL,
        internal_tx_id VARCHAR(64),
        provider_tx_id VARCHAR(64),
        discrepancy_type VARCHAR(50) NOT NULL,
        internal_amount NUMERIC(12, 2),
        external_amount NUMERIC(12, 2),
        flagged_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_audit_batch_date ON reconciliation_audit(batch_date);
    """
    hook.run(schema_sql)
    logger.info("Schemas initialized successfully.")


def load_internal_csv_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    with open("/opt/airflow/data/internal_feed.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cursor.execute(
                """
                INSERT INTO internal_transactions (
                    transaction_id, account_id, amount, currency, status, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING;
                """,
                (
                    row["transaction_id"],
                    row["account_id"],
                    float(row["amount"]),
                    row["currency"],
                    row["status"],
                    row["created_at"],
                ),
            )
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Internal transactions ingested.")


def load_external_csv_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    with open("/opt/airflow/data/external_feed.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cursor.execute(
                """
                INSERT INTO external_gateway_transactions (
                    provider_tx_id, internal_reference_id, cleared_amount, currency, settlement_status, cleared_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (provider_tx_id) DO NOTHING;
                """,
                (
                    row["provider_tx_id"],
                    row["internal_reference_id"],
                    float(row["cleared_amount"]),
                    row["currency"],
                    row["settlement_status"],
                    row["cleared_at"],
                ),
            )
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("External gateway records ingested.")


def execute_reconciliation_logic():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    reconciliation_sql = """
    INSERT INTO reconciliation_audit (
        batch_date,
        internal_tx_id,
        provider_tx_id,
        discrepancy_type,
        internal_amount,
        external_amount
    )
    SELECT 
        CURRENT_DATE AS batch_date,
        it.transaction_id AS internal_tx_id,
        eg.provider_tx_id AS provider_tx_id,
        CASE
            WHEN it.transaction_id IS NULL THEN 'MISSING_IN_INTERNAL_LEDGER'
            WHEN eg.provider_tx_id IS NULL THEN 'MISSING_IN_GATEWAY'
            WHEN it.amount <> eg.cleared_amount THEN 'AMOUNT_MISMATCH'
            ELSE 'MATCHED'
        END AS discrepancy_type,
        it.amount AS internal_amount,
        eg.cleared_amount AS external_amount
    FROM internal_transactions it
    FULL OUTER JOIN external_gateway_transactions eg
        ON it.transaction_id = eg.internal_reference_id
    WHERE it.transaction_id IS NULL 
       OR eg.provider_tx_id IS NULL 
       OR it.amount <> eg.cleared_amount;
    """
    hook.run(reconciliation_sql)
    logger.info("Reconciliation analysis executed.")


def log_reconciliation_summary():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    records = hook.get_records(
        """
        SELECT discrepancy_type, COUNT(*) 
        FROM reconciliation_audit 
        GROUP BY discrepancy_type;
        """
    )
    logger.info("=== RECONCILIATION SUMMARY ===")
    for disc_type, count in records:
        logger.info(f"Anomaly [{disc_type}]: {count} records")


with DAG(
    dag_id="automated_financial_reconciliation",
    default_args=default_args,
    description="Daily ingestion, validation, and full outer reconciliation of internal ledger vs external gateway",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    create_schemas = PythonOperator(
        task_id="create_schemas",
        python_callable=init_database_schemas,
    )

    ingest_internal_feed = PythonOperator(
        task_id="ingest_internal_feed",
        python_callable=load_internal_csv_to_postgres,
    )

    ingest_external_feed = PythonOperator(
        task_id="ingest_external_feed",
        python_callable=load_external_csv_to_postgres,
    )

    run_reconciliation = PythonOperator(
        task_id="run_reconciliation",
        python_callable=execute_reconciliation_logic,
    )

    audit_summary = PythonOperator(
        task_id="audit_summary",
        python_callable=log_reconciliation_summary,
    )

    create_schemas >> [ingest_internal_feed, ingest_external_feed] >> run_reconciliation >> audit_summary