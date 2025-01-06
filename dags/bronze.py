from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
import os

def process_bronze_layer(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("ETL - Tabelas Bronze").getOrCreate()

    try:
        # Caminhos dos arquivos CSV
        path_receitas = f"{input_path}/gdvReceitasExcel.csv"
        path_despesas = f"{input_path}/gdvDespesasExcel.csv"

        # Verificar se os arquivos existem
        if not os.path.exists(path_receitas):
            raise FileNotFoundError(f"Arquivo de receitas não encontrado: {path_receitas}")
        if not os.path.exists(path_despesas):
            raise FileNotFoundError(f"Arquivo de despesas não encontrado: {path_despesas}")

        # Ler os arquivos CSV
        receitas = spark.read.option("header", True).csv(path_receitas, encoding="latin1")
        despesas = spark.read.option("header", True).csv(path_despesas, encoding="latin1")

        # Processar receitas
        receitas_clean = receitas.withColumn(
            "Arrecadado até",
            regexp_replace(regexp_replace(col("Arrecadado até 02/02/2024"), r"\.", ""), ",", ".").cast("float")
        ).drop("Arrecadado até 02/02/2024")

        # Verificar e remover colunas desnecessáriass
        if "_c3" in receitas_clean.columns:
            receitas_clean = receitas_clean.drop("_c3")
        
        # Processar despesas
        despesas_clean = despesas.withColumn(
            "Liquidado",
            regexp_replace(regexp_replace(col("Liquidado"), r"\.", ""), ",", ".").cast("float")
        )

        if "_c3" in despesas_clean.columns:
            despesas_clean = despesas_clean.drop("_c3")

        # Salvar os dados no formato CSV
        receitas_clean.write.mode("overwrite").option("header", "true").csv(f"{output_path}/bronze/receitas")
        despesas_clean.write.mode("overwrite").option("header", "true").csv(f"{output_path}/bronze/despesas")

        print("Processamento da camada Bronze concluído com sucesso!")

    except Exception as e:
        print(f"Erro durante o processamento da camada Bronze: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    process_bronze_layer("/opt/airflow/data", "/opt/airflow/output")
