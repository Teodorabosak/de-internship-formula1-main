import requests
import json
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def fetch_and_push(endpoint_name, kafka_topic):
    # endpoint_name može biti 'drivers', 'constructors', 'results'
    url = f"https://api.jolpi.ca/ergast/f1/2026/{endpoint_name}.json"
    response = requests.get(url)
    data = response.json()
    
    # Dinamički pronalazimo ključ u JSON-u (npr. 'DriverTable' ili 'ConstructorTable')
    # API obično vraća podatke pod ključem koji odgovara nazivu endpointa
    producer = KafkaProducer(bootstrap_servers=['kafka1:9092'])
    producer.send(kafka_topic, value=data) 
    producer.flush()
#To je tvoja Python skripta ili Airflow task. 
# Njegov jedini posao je da uzme podatak (npr. JSON o vozaču sa API-ja) i "okači" ga na Kafku. On ne brine o tome ko će taj podatak pročitati.
# U Airflow-u samo ređaš taskove:
t1 = PythonOperator(
    task_id='ingest_drivers',
    python_callable=fetch_and_push,
    op_kwargs={'endpoint_name': 'drivers', 'kafka_topic': 'f1_drivers'}
)

t2 = PythonOperator(
    task_id='ingest_results',
    python_callable=fetch_and_push,
    op_kwargs={'endpoint_name': 'results', 'kafka_topic': 'f1_results'}
)