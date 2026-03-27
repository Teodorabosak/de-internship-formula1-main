import json
import time
import requests
from airflow.decorators import dag, task
from kafka import KafkaProducer
from pendulum import datetime


KAFKA_BROKERS = ['kafka1:9092']
# spisak adresa na kojima se nalaze podaci
# Base URL for results
API_URL = {
    "status": "https://ergast.com/api/f1/status.json",
    "driver": "http://api.jolpi.ca/ergast/f1/drivers.json",
    "race": "http://api.jolpi.ca/ergast/f1/races.json",
    "constructor": "https://ergast.com/api/f1/constructors.json",
    "laptimes": "https://ergast.com/api/f1/2024",
    "driverstandings": "https://ergast.com/api/f1/2024/driverStandings.json",
    "constandings": "https://ergast.com/api/f1/2024/constructorStandings.json",
    "pitstops": "https://ergast.com/api/f1/2024",
    "results": "https://ergast.com/api/f1/2024" 
}
 #ovde razvrstavamo u topics 
TOPICS = {
    "status": "status_topic",
    "driver": "driver_topic",
    "race": "race_topic",
    "constructor": "constructor_topic",
    "laptimes": "laptimes_topic",
    "driverstandings": "driverstandings_topic",
    "constandings": "constandings_topic",
    "pitstops": "pitstops_topic",
    "results": "results_topic"  
}

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8')
    )

def fetch_with_retry(url):
    for attempt in range(3):
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
        time.sleep(2 ** attempt)
    raise Exception(f"API failed: {url}")

@dag(
    dag_id="api_to_kafka_clean",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
)
def api_to_kafka():

    @task
    def extract_and_send(endpoint_name, url):
        producer = create_producer()

        data = fetch_with_retry(url)

        # dynamic extraction
        records = data["MRData"]
        for key in ["DriverTable", "ConstructorTable"]:
            if key in records:
                records = records[key]

        records = list(records.values())[0]

        for record in records:
            kafka_key = record.get("driverId") or record.get("constructorId")
            producer.send(
                TOPICS[endpoint_name],
                key=kafka_key,
                value=record
            )

        producer.flush()
        producer.close()

    for name, url in API_URL.items():
        extract_and_send(name, url)

dag = api_to_kafka()