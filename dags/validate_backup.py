import os
import sys
import zipfile

def ensure_permissions(path):
    """
    Garante que o diretório e seus arquivos tenham permissões adequadas.
    """
    try:
        os.chmod(path, 0o777)
        for root, dirs, files in os.walk(path):
            for name in dirs:
                os.chmod(os.path.join(root, name), 0o777)
            for name in files:
                os.chmod(os.path.join(root, name), 0o777)
        print(f"Permissões ajustadas para: {path}")
    except Exception as e:
        print(f"Erro ao ajustar permissões: {e}")

def validate_backup(backup_path):
    """
    Valida o conteúdo do backup.
    """
    if not os.path.exists(backup_path):
        print(f"Backup não encontrado no caminho especificado: {backup_path}")
        return

    # Descompactar o arquivo de backup
    try:
        latest_backup = max(
            [os.path.join(backup_path, f) for f in os.listdir(backup_path) if f.endswith(".zip")],
            key=os.path.getctime
        )
        print(f"Validando o backup mais recente: {latest_backup}")

        extract_path = "/tmp/test_backup"
        if not os.path.exists(extract_path):
            os.makedirs(extract_path)

        # Garantir permissões adequadas no diretório de extração
        ensure_permissions(extract_path)

        with zipfile.ZipFile(latest_backup, 'r') as zip_ref:
            zip_ref.extractall(extract_path)

        # Listar os arquivos extraídos
        print("Arquivos encontrados no backup:")
        for root, dirs, files in os.walk(extract_path):
            for name in dirs:
                print(f" - {os.path.join(root, name)}")
            for name in files:
                print(f" - {os.path.join(root, name)}")

        print("Validação do backup concluída com sucesso!")

    except Exception as e:
        print(f"Erro ao validar o backup: {e}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python validate_backup.py <caminho_dos_backups>")
        sys.exit(1)

    backup_path = sys.argv[1]
    validate_backup(backup_path)
