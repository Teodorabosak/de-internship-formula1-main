from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta


default_args = {
    "owner": "teodora",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="raw_to_staging",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    # =================================================
    # DIMENSIONS (parallel group)
    # =================================================
    with TaskGroup("load_dimensions") as load_dimensions:

        load_circuits = SQLExecuteQueryOperator(
            task_id="circuits",
            conn_id="postgres_formula",
            sql="sql/staging/circuits.sql"
        )

        load_constructors = SQLExecuteQueryOperator(
            task_id="constructors",
            conn_id="postgres_formula",
            sql="sql/staging/constructors.sql"
        )

        load_drivers = SQLExecuteQueryOperator(
            task_id="drivers",
            conn_id="postgres_formula",
            sql="sql/staging/drivers.sql"
        )

        load_status = SQLExecuteQueryOperator(
            task_id="status",
            conn_id="postgres_formula",
            sql="sql/staging/status.sql"
        )

    # =================================================
    # SESSIONS (parallel group)
    # =================================================
    with TaskGroup("load_sessions") as load_sessions:

        load_fp = SQLExecuteQueryOperator(
            task_id="free_practice",
            conn_id="postgres_formula",
            sql="sql/staging/free_practice.sql"
        )

        load_qual = SQLExecuteQueryOperator(
            task_id="qualifying",
            conn_id="postgres_formula",
            sql="sql/staging/qualifying.sql"
        )

        load_sprint = SQLExecuteQueryOperator(
            task_id="sprint",
            conn_id="postgres_formula",
            sql="sql/staging/sprint.sql"
        )

    # =================================================
    # RACES
    # =================================================
    load_races = SQLExecuteQueryOperator(
        task_id="races",
        conn_id="postgres_formula",
        sql="sql/staging/races.sql"
    )

    # =================================================
    # RESULTS 
    # =================================================
    load_results = SQLExecuteQueryOperator(
        task_id="results",
        conn_id="postgres_formula",
        sql="sql/staging/results.sql"
    )

    # =================================================
    # FACT TABLES (parallel group)
    # =================================================
    with TaskGroup("load_fact_tables") as load_fact_tables:

        load_pitstops = SQLExecuteQueryOperator(
            task_id="pitstops",
            conn_id="postgres_formula",
            sql="sql/staging/pitstops.sql"
        )

        load_driver_standings = SQLExecuteQueryOperator(
            task_id="driver_standings",
            conn_id="postgres_formula",
            sql="sql/staging/driver_standings.sql"
        )

        load_constructor_standings = SQLExecuteQueryOperator(
            task_id="constructor_standings",
            conn_id="postgres_formula",
            sql="sql/staging/constructor_standings.sql"
        )

    # =================================================
    # DEPENDENCIES
    # =================================================

    # dimensions + sessions paralelno → races
    load_dimensions >> load_races
    
    load_races >> load_sessions

    # races → results
    load_sessions >> load_results

    # results → fact tables (paralelno)
    load_results >> load_fact_tables