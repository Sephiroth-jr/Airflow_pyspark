from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

def process_silver_layer(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("ETL - Tabelas Silver").getOrCreate()

    try:
        for table in ["receitas", "despesas"]:
            bronze_df = spark.read.option("header", "true").csv(f"{input_path}/bronze/{table}")

            # Deduplicar e ajustar tipos
            silver_df = bronze_df.dropDuplicates()

            # Remover colunas nulas
            silver_df = silver_df.dropna(how="all")

            if "Arrecadado até" in silver_df.columns:
                silver_df = silver_df.withColumn("Arrecadado até", col("Arrecadado até").cast("float"))

            if "Liquidado" in silver_df.columns:
                silver_df = silver_df.withColumn("Liquidado", col("Liquidado").cast("float"))

            # Salvar os dados no formato CSV
            silver_df.write.mode("overwrite").option("header", "true").csv(f"{output_path}/silver/{table}")

            print(f"Processamento da tabela {table} concluído com sucesso!")

    except Exception as e:
        print(f"Erro durante o processamento da camada Silver: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    process_silver_layer("/opt/airflow/output", "/opt/airflow/output")
