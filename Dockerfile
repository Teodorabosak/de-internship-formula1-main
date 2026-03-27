FROM apache/airflow:2.9.2

USER airflow

# Postavi Airflow constraints URL
ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY requirements.txt .

# Instaliraj pakete kao airflow user sa constraints
RUN pip install --no-cache-dir --constraint "${CONSTRAINT_URL}" -r requirements.txt