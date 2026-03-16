FROM apache/airflow:2.8.0

USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk \
    && mkdir -p /opt/spark-events && chown 50000:0 /opt/spark-events

USER airflow
COPY airflow/requirements-docker.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt