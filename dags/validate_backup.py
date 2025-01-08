import os
import shutil
from pathlib import Path

def validate_backup(backup_folder: str, validation_folder: str):
    """
    Valida os arquivos de backup e os move para a pasta de validação.

    Args:
        backup_folder (str): Caminho da pasta onde os backups estão armazenados.
        validation_folder (str): Caminho da pasta onde backups validados serão armazenados.
    """
    try:
        # Criar a pasta de validação, se não existir
        os.makedirs(validation_folder, exist_ok=True)

        # Listar arquivos na pasta de backup
        for file_name in os.listdir(backup_folder):
            if file_name.endswith(".zip"):
                src_path = os.path.join(backup_folder, file_name)
                dest_path = os.path.join(validation_folder, file_name)

                # Mover arquivo validado para a pasta de validação
                shutil.move(src_path, dest_path)
                print(f"Arquivo validado e movido: {file_name}")

    except Exception as e:
        print(f"Erro durante a validação de backup: {e}")

# Teste do script
if __name__ == "__main__":
    validate_backup("/opt/airflow/backups", "/opt/airflow/validation")
