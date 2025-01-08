from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator  # Atualizado para evitar depreciação
from datetime import datetime
import os
import shutil
from datetime import datetime as dt

# Função para realizar backup
def backup_data(output_path: str, backup_path: str):
    try:
        if not os.path.exists(backup_path):
            os.makedirs(backup_path)

        # Criar um nome único para o arquivo de backup
        timestamp = dt.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(backup_path, f"backup_{timestamp}.zip")

        # Compactar a pasta de saída
        shutil.make_archive(backup_file.replace(".zip", ""), 'zip', output_path)

        print(f"Backup criado com sucesso: {backup_file}")
    except Exception as e:
        print(f"Erro ao criar o backup: {e}")

# Configurações padrão
default_args = {
    'start_date': datetime(2023, 1, 1),
}

# DAG Principal
with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    schedule='@daily',  # Atualizado para evitar depreciação
    catchup=False,
) as dag:

    # Tarefa Bronze
    bronze_task = BashOperator(
        task_id="bronze_layer",
        bash_command="python /opt/airflow/dags/bronze.py /opt/airflow/data /opt/airflow/output"
    )

    # Tarefa Silver
    silver_task = BashOperator(
        task_id="silver_layer",
        bash_command="python /opt/airflow/dags/silver.py /opt/airflow/output/bronze /opt/airflow/output"
    )

    # Tarefa Gold
    gold_task = BashOperator(
        task_id="gold_layer",
        bash_command="python /opt/airflow/dags/gold.py /opt/airflow/output/silver /opt/airflow/output"
    )

    # Tarefa de Backup
    backup_task = PythonOperator(
        task_id='backup_data',
        python_callable=backup_data,
        op_kwargs={
            'output_path': '/opt/airflow/output',
            'backup_path': '/opt/airflow/backups',
        }
    )

    # Tarefa de Validação
    validate_task = BashOperator(
        task_id="validate_backup",
        bash_command="python /opt/airflow/dags/validate_backup.py /opt/airflow/backups /opt/airflow/validation"  # Ajustado para incluir a pasta de validação como argumento
    )

    # Tarefa de Exportação para PostgreSQL
    export_to_postgres_task = BashOperator(
        task_id="export_to_postgres",
        bash_command="python /opt/airflow/dags/export_to_postgres.py"
    )

    # Ordem de execução
    bronze_task >> silver_task >> gold_task >> backup_task >> validate_task >> export_to_postgres_task
