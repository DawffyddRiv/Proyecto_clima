FROM apache/airflow:2.7.3

USER root

# Instalación de Java 11
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow

# Instalación de librerías con restricciones de versión para evitar conflictos
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.1.1 \
    pyspark==3.4.0 \
    pandas \
    psycopg2-binary \
    sqlalchemy