from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import os


def load_csv():

    # čitanje CSV iz env
    df = pd.read_csv(
        os.getenv("CSV_PATH"),
        dtype=str
    )

    # lowercase kolone
    df.columns = df.columns.str.lower()

    # konekcija iz env
    engine = create_engine(os.getenv("DB_URL"))

    # truncate tabela iz env
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {os.getenv('RAW_TABLE')}"))

    # load u bazu
    df.to_sql(
        "raw_dataset",
        engine,
        schema="raw",
        if_exists="append",
        index=False
    )


with DAG(
    dag_id="load_csv_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    load_task = PythonOperator(
        task_id="load_csv",
        python_callable=load_csv
    )