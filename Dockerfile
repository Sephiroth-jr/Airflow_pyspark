FROM apache/airflow:2.10.0-python3.8

USER root

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
RUN apt-get update && apt-get install -y python3 python3-pip \
    python3-dev \
    default-jdk \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configurar permissões do diretório Airflow
RUN mkdir -p /opt/airflow && \
    chmod -R 777 /opt/airflow && \
    chown -R 50000:0 /opt/airflow

# Mudar para o usuário airflow para executar comandos pip
USER 50000

# Instalar dependências Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir apache-airflow[postgres,aws]==2.10.0 \
    pyspark \
    psycopg2-binary

# Configuração padrão
ENV AIRFLOW_HOME=/opt/airflow
COPY ./dags $AIRFLOW_HOME/dags/
COPY ./plugins $AIRFLOW_HOME/plugins/
COPY ./validation $AIRFLOW_HOME/validation/
