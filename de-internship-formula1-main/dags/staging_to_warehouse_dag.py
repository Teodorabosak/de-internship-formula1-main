from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup 
#grupise taskove
from datetime import datetime, timedelta


# =====================================================
# DEFAULT ARGS
# =====================================================

default_args = {
    "owner": "teodora",
    "retries": 2, #koliko puta pokusa ako padne
    "retry_delay": timedelta(minutes=2), #pauza izmedju 
}


with DAG(
    dag_id="staging_to_warehouse", #ime 
    start_date=datetime(2024, 1, 1),
    schedule_interval=None, #rucno se pokrece
    catchup=False, #ne izvrsava stare datume
    default_args=default_args
) as dag:

    with TaskGroup("load_dimensions") as load_dimensions:

        load_circuits = SQLExecuteQueryOperator(
            task_id="circuits",
            conn_id="postgres_formula",
            sql="sql/warehouse/dim_circuits.sql"
        )

        load_constructors = SQLExecuteQueryOperator(
            task_id="constructors",
            conn_id="postgres_formula",
            sql="sql/warehouse/dim_constructors.sql"
        )

        load_drivers = SQLExecuteQueryOperator(
            task_id="drivers",
            conn_id="postgres_formula",
            sql="sql/warehouse/dim_drivers.sql"
        )

        load_status = SQLExecuteQueryOperator(
            task_id="status",
            conn_id="postgres_formula",
            sql="sql/warehouse/dim_status.sql"
        )
with TaskGroup("load_sessions") as load_sessions:

        load_fp = SQLExecuteQueryOperator(
            task_id="free_practice",
            conn_id="postgres_formula",
            sql="sql/warehouse/dim_free_practice.sql"
        )

        load_qual = SQLExecuteQueryOperator(
            task_id="qualifying",
            conn_id="postgres_formula",
            sql="sql/warehouse/dim_qualifying.sql"
        )

        load_sprint = SQLExecuteQueryOperator(
            task_id="sprint",
            conn_id="postgres_formula",
            sql="sql/warehouse/dim_sprint.sql"
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
    # FACT PREP
    # =================================================

        load_results = SQLExecuteQueryOperator(
        task_id="results",
        conn_id="postgres_formula",
        sql="sql/staging/results.sql"
    )

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

        load_dimensions >> load_races
        load_sessions >> load_races

        load_races >> load_results

        load_results >> [
        load_pitstops,
        load_driver_standings,
        load_constructor_standings
    ]        