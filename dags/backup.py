import os
import shutil
from datetime import datetime

def backup_data(output_path: str, backup_path: str):
    try:
        if not os.path.exists(backup_path):
            os.makedirs(backup_path)

        # Criar um nome único para o arquivo de backup
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(backup_path, f"backup_{timestamp}.zip")

        # Compactar a pasta de saída
        shutil.make_archive(backup_file.replace(".zip", ""), 'zip', output_path)

        print(f"Backup criado com sucesso: {backup_file}")
    except Exception as e:
        print(f"Erro ao criar o backup: {e}")

if __name__ == "__main__":
    backup_data("/opt/airflow/output", "/opt/airflow/backups")
