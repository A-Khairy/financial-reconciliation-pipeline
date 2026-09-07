# Automated Financial Reconciliation Pipeline

An enterprise-grade financial data pipeline orchestrating daily cross-system reconciliation between core ledger balances and external payment gateway clearing feeds using **Apache Airflow**, **PostgreSQL**, and **Docker**.

---

## System Architecture

```text
               +-----------------------------+
               |       Airflow DAG           |
               |  (automated_reconciliation) |
               +--------------+--------------+
                              |
               +--------------v--------------+
               |    1. init_database_schemas  |
               +--------------+--------------+
                              |
              +---------------+---------------+
              |                               |
    +---------v-----------+       +-----------v---------+
    | 2. ingest_internal  |       | 3. ingest_external  |
    | (Internal CSV Feed) |       | (Gateway CSV Feed)  |
    +---------+-----------+       +-----------+---------+
              |                               |
              +---------------+---------------+
                              |
               +--------------v--------------+
               |  4. run_reconciliation (SQL)|
               |    (Full Outer Join Audit)  |
               +--------------+--------------+
                              |
               +--------------v--------------+
               |   5. log_audit_summary      |
               +-----------------------------+
               Core Technical Features
Orchestration & Idempotency: Managed via Airflow LocalExecutor with atomic transaction imports (ON CONFLICT DO NOTHING).

Reconciliation Engine: SQL Full Outer Join flagging:

AMOUNT_MISMATCH: Deviation between billed and settled amounts.

MISSING_IN_GATEWAY: Internal charges lacking external clearing confirmation.

MISSING_IN_INTERNAL_LEDGER: Gateway charges lacking internal journal logs.

Audit Ledger: Partition-ready table (reconciliation_audit) indexing batch dates and discrepancies.

Setup & Execution
1. Start Stack
docker compose up -d
2. Generate Synthetic Datasets
python data/generate_mock_data.py
3. Run DAG
docker compose exec airflow-webserver airflow dags trigger automated_financial_reconciliation
4. Query Reconciliation Results
docker compose exec postgres psql -U airflow -d airflow -c "SELECT discrepancy_type, COUNT(*) FROM reconciliation_audit GROUP BY discrepancy_type;"
