from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

#ovo je 1:1 sa csv, samo da se migrira u bazu posle treba srediti
def load_csv():

    df = pd.read_csv(
    "/opt/airflow/data/raw/dataEngineeringDataset.csv",
    na_values="\\N",
    dtype={
        "resultId": "Int64",
        "raceId": "Int64",
        "driverId": "Int64",
        "constructorId": "Int64",
        "circuitId": "Int64"
    }
    )

    # uklanja pandas index kolone
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    # sve kolone u lowercase
    df.columns = df.columns.str.lower()
    engine = create_engine(
        "postgresql://postgres:postgres@postgres_formula:5432/formula_one_db"
    )
    
    with engine.begin() as conn:
        conn.execute("TRUNCATE TABLE raw.raw_dataset")
    
    df.to_sql(
    "raw_dataset",
    engine,
    schema="raw",
    if_exists="append",
    index=False
)


with DAG(
    dag_id="load_csv_to_postgres",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False
) as dag:
#python operator ide jer imamo funkciju 
    load_task = PythonOperator(
        task_id="load_csv",
        python_callable=load_csv
    )