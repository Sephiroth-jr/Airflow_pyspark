import psycopg2
import pandas as pd
import os
import numpy as np

def export_to_postgres(db_config: dict, data_folder: str):
    """
    Exporta dados das tabelas maiores_despesas e maiores_receitas para o PostgreSQL.

    Args:
        db_config (dict): Configurações do banco de dados PostgreSQL.
        data_folder (str): Pasta onde os arquivos CSV das tabelas estão localizados.
    """
    try:
        # Conexão com o banco de dados
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Dicionário de tabelas e arquivos CSV correspondentes
        tables = {
            "maiores_despesas": {
                "file": "maiores_despesas.csv",
                "schema": '''
                    CREATE TABLE IF NOT EXISTS maiores_despesas (
                        "Fonte de Recursos" TEXT,
                        "Total Liquidado" NUMERIC
                    )
                '''
            },
            "maiores_receitas": {
                "file": "maiores_receitas.csv",
                "schema": '''
                    CREATE TABLE IF NOT EXISTS maiores_receitas (
                        "Fonte de Recursos" TEXT,
                        "Total Arrecadado" NUMERIC
                    )
                '''
            }
        }

        for table_name, table_info in tables.items():
            # Criar a tabela se não existir
            cursor.execute(table_info["schema"])
            conn.commit()

            # Caminho do arquivo
            file_path = os.path.join(data_folder, table_info["file"])

            # Verificar se o arquivo existe
            if os.path.exists(file_path):
                # Carregar o arquivo CSV em um DataFrame
                df = pd.read_csv(file_path)

                # Substituir NaN por None no DataFrame
                df = df.replace({np.nan: None})

                # Exportar os dados para o PostgreSQL
                for _, row in df.iterrows():
                    # Verificar se o registro já existe
                    where_clause = " AND ".join(
                        [f'"{col}" = %s' if row[col] is not None else f'"{col}" IS NULL' for col in df.columns]
                    )
                    check_query = f"SELECT 1 FROM {table_name} WHERE {where_clause} LIMIT 1"
                    cursor.execute(check_query, tuple(val for val in row if val is not None))

                    # Se não existir, insere o registro
                    if not cursor.fetchone():
                        columns = ', '.join([f'"{col}"' for col in df.columns])
                        values = ', '.join([f"%s" for _ in df.columns])
                        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
                        cursor.execute(insert_query, tuple(row))

                print(f"Dados exportados com sucesso para a tabela {table_name}.")
            else:
                print(f"Arquivo {table_info['file']} não encontrado.")

        # Confirmar as mudanças
        conn.commit()

    except Exception as e:
        print(f"Erro ao exportar dados para o PostgreSQL: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

# Configuração do banco e chamada do script
if __name__ == "__main__":
    db_config = {
        "dbname": "airflow",
        "user": "airflow",
        "password": "airflow",
        "host": "postgres",
        "port": "5432"
    }
    data_folder = "/opt/airflow/output/gold"
    export_to_postgres(db_config, data_folder)
