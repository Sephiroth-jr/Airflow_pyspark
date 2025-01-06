from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import os

def process_gold_layer(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("ETL - Tabelas Gold").getOrCreate()

    try:
        # Ler os dados processados na camada Silver
        receitas_silver = spark.read.option("header", "true").csv(f"{input_path}/silver/receitas")
        despesas_silver = spark.read.option("header", "true").csv(f"{input_path}/silver/despesas")

        # Agregar maiores receitas
        maiores_receitas = receitas_silver.groupBy("Fonte de Recursos").agg(
            _sum("Arrecadado até").alias("Total Arrecadado")
        ).orderBy(col("Total Arrecadado").desc())

        # Agregar maiores despesas
        maiores_despesas = despesas_silver.groupBy("Despesa").agg(
            _sum("Liquidado").alias("Total Liquidado")
        ).orderBy(col("Total Liquidado").desc())

        # Salvar os dados no formato CSV
        maiores_receitas.write.mode("overwrite").option("header", "true").csv(f"{output_path}/gold/maiores_receitas")
        maiores_despesas.write.mode("overwrite").option("header", "true").csv(f"{output_path}/gold/maiores_despesas")

        print("Processamento da camada Gold concluído com sucesso!")

    except Exception as e:
        print(f"Erro durante o processamento da camada Gold: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    process_gold_layer("/opt/airflow/output", "/opt/airflow/output")
