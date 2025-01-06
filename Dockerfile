FROM apache/airflow:2.6.3-python3.10

# Instalar dependências do sistema
USER root
RUN mkdir -p /opt/airflow/output/bronze/receitas && chmod -R 777 /opt/airflow/output
RUN apt-get update && apt-get install -y procps
RUN apt-get update && apt-get install -y \
    postgresql-client && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt-get install -y unzip
RUN apt-get update && apt-get install -y \
    default-jdk \
    libpq-dev && \
    apt-get clean

# Mudar para o usuário airflow para instalar dependências Python
USER airflow
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt