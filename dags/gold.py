from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import os
import glob
import shutil

def process_gold_layer(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("ETL - Tabelas Gold").getOrCreate()

    try:
        # Ler os dados processados na camada Silver
        receitas_silver = spark.read.option("header", "true").csv(f"{input_path}/receitas")
        despesas_silver = spark.read.option("header", "true").csv(f"{input_path}/despesas")

        # Agregar maiores receitas
        maiores_receitas = receitas_silver.groupBy("Fonte de Recursos").agg(
            _sum("Arrecadado até").alias("Total Arrecadado")
        ).orderBy(col("Total Arrecadado").desc())

        # Agregar maiores despesas
        maiores_despesas = despesas_silver.groupBy("Fonte de Recursos").agg(
            _sum("Liquidado").alias("Total Liquidado")
        ).orderBy(col("Total Liquidado").desc())

        # Caminhos temporários de saída
        temp_receitas_path = os.path.join(output_path, "gold_temp_receitas")
        temp_despesas_path = os.path.join(output_path, "gold_temp_despesas")

        # Caminhos finais para os arquivos CSV
        final_receitas_csv = os.path.join(output_path, "gold", "maiores_receitas.csv")
        final_despesas_csv = os.path.join(output_path, "gold", "maiores_despesas.csv")

        # Remover diretórios ou arquivos antigos, se existirem
        for path in [final_receitas_csv, final_despesas_csv, temp_receitas_path, temp_despesas_path]:
            if os.path.exists(path):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)

        # Salvar dados agregados no formato CSV
        maiores_receitas.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_receitas_path)
        maiores_despesas.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_despesas_path)

        # Renomear e mover o arquivo de receitas
        temp_receitas_files = glob.glob(os.path.join(temp_receitas_path, "*.csv"))
        if temp_receitas_files:
            shutil.move(temp_receitas_files[0], final_receitas_csv)

        # Renomear e mover o arquivo de despesas
        temp_despesas_files = glob.glob(os.path.join(temp_despesas_path, "*.csv"))
        if temp_despesas_files:
            shutil.move(temp_despesas_files[0], final_despesas_csv)

        # Remover diretórios temporários
        for path in [temp_receitas_path, temp_despesas_path]:
            if os.path.exists(path):
                shutil.rmtree(path)

        print("Processamento da camada Gold concluído com sucesso!")

    except Exception as e:
        print(f"Erro durante o processamento da camada Gold: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    process_gold_layer("/opt/airflow/output/silver", "/opt/airflow/output")
