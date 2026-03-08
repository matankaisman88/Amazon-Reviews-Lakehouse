# Amazon Reviews Lakehouse - Spark 3.5 + Delta Lake + GX
FROM apache/spark:3.5.0-python3

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    "urllib3<2.0.0" \
    "delta-spark==3.2.0" \
    "great-expectations>=0.18.0" \
    "python-dotenv>=1.0.0" \
    "pyyaml>=6.0" \
    "boto3>=1.28.0" \
    "streamlit>=1.36.0" \
    "plotly>=5.20.0" \
    "deltalake>=0.17.0" \
    "openai>=1.0.0,<2.0.0"

USER ${spark_uid}

# Project code mounted at /opt/spark-app
WORKDIR /opt/spark-app

ENV PYTHONPATH=/opt/spark-app
ENV SPARK_HOME=/opt/spark

# Event log for History Server
ENV SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=/opt/spark-events"

EXPOSE 7077 8080 8081 18080 8501
