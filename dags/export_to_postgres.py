from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import psycopg2


def export_gold_to_postgres(input_path: str, db_params: dict):
    spark = SparkSession.builder.appName("Exportar para PostgreSQL - Gold").getOrCreate()

    try:
        # Ler os dados processados na camada Gold
        gold_receitas_path = f"{input_path}/gold/maiores_receitas"
        gold_despesas_path = f"{input_path}/gold/maiores_despesas"

        if not os.path.exists(gold_receitas_path) or not os.path.exists(gold_despesas_path):
            raise FileNotFoundError("Os arquivos na camada Gold não foram encontrados.")

        gold_receitas = spark.read.csv(gold_receitas_path, header=True, inferSchema=True).withColumn("camada", lit("gold"))
        gold_despesas = spark.read.csv(gold_despesas_path, header=True, inferSchema=True).withColumn("camada", lit("gold"))

        # Consolidar os dados
        consolidated_data = gold_receitas.union(gold_despesas)

        # Conectar ao banco PostgreSQL
        conn = psycopg2.connect(
            host=db_params['host'],
            database=db_params['database'],
            user=db_params['user'],
            password=db_params['password'],
            port=db_params['port']
        )
        cursor = conn.cursor()

        # Criar tabela "gold_output" caso não exista
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gold_output (
                camada TEXT,
                coluna1 TEXT,
                coluna2 FLOAT,
                coluna3 FLOAT,
                PRIMARY KEY (camada, coluna1)
            )
        """)

        # Inserir dados na tabela "gold_output"
        for row in consolidated_data.collect():
            cursor.execute("""
                INSERT INTO gold_output (camada, coluna1, coluna2, coluna3)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (camada, coluna1) DO UPDATE SET
                    coluna2 = EXCLUDED.coluna2,
                    coluna3 = EXCLUDED.coluna3
            """, (row["camada"], row["coluna1"], row.get("coluna2"), row.get("coluna3")))

        conn.commit()
        print("Dados exportados com sucesso para a tabela gold_output no PostgreSQL!")

    except Exception as e:
        print(f"Erro durante a exportação para PostgreSQL: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()
        spark.stop()

if __name__ == "__main__":
    db_params = {
        'host': 'localhost',
        'database': 'projeto_etl',
        'user': 'airflow',
        'password': 'airflow',
        'port': '5432'
    }
    export_gold_to_postgres("/opt/airflow/output", db_params)

