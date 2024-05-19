FROM python:3.11

RUN apt-get update && apt-get install -y\
    build-essential \
    curl \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=${PYTHONPATH}:/opt/airflow/scripts

RUN pip install apache-airflow

COPY dags/ $AIRFLOW_HOME/dags/
COPY config/ $AIRFLOW_HOME/config/
COPY scripts/ $AIRFLOW_HOME/scripts/

COPY docker-entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

COPY requirements.txt .
RUN pip install -r requirements.txt

ENTRYPOINT ["/entrypoint.sh"]

CMD ["webserver"]