import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, concat_ws, lit, when, upper, trim, year
)
from pyspark.sql.types import IntegerType

def main():
    print("Iniciando processo de Bronze para Silver...")
    
    # 1. Criar a SparkSession
    # Usamos "local[*]" para rodar localmente usando todos os cores da VM
    spark = (
        SparkSession.builder.appName("ProcessamentoBronzeParaSilver")
        .master("local[*]")
        .getOrCreate()
    )

    # 2. Configurar o Spark para falar com o MinIO (S3)
    # Essas configurações são para o driver S3A que o Spark usa
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    print("Spark Session criada e configurada para MinIO.")

    # 3. Definir caminhos de Leitura (Bronze) e Escrita (Silver)
    # O Spark vai ler TODOS os arquivos CSV dentro do bucket bronze
    BRONZE_PATH = "s3a://bronze/*.csv" 
    SILVER_PATH = "s3a://silver/acidentes_limpos"

    # 4. Ler os dados do bucket Bronze
    # ATENÇÃO: Verifique se o seu separador é mesmo ';'
    df_bronze = (
        spark.read.csv(
            BRONZE_PATH,
            header=True,
            inferSchema=False, # Vamos definir os tipos manualmente
            sep=';'             # Mude para ',' se necessário
        )
    )
    
    print(f"Dados lidos do bucket bronze. Total de {df_bronze.count()} linhas.")

    # 5. Aplicar Transformações (Limpeza)
    
    # Lista de colunas numéricas que queremos converter e tratar nulos
    colunas_metricas = [
        "pessoas", "mortos", "feridos_leves", "feridos_graves", 
        "ilesos", "ignorados", "feridos", "veiculos"
    ]
    
    df_silver = df_bronze

    # Converter métricas para Integer e preencher nulos com 0
    for c in colunas_metricas:
        df_silver = df_silver.withColumn(c, col(c).cast(IntegerType()))
        df_silver = df_silver.withColumn(c, when(col(c).isNull(), 0).otherwise(col(c)))
        
    # Limpar e Padronizar colunas de texto (Dimensões)
    colunas_texto = [
        "uf", "br", "municipio", "causa_acidente", "tipo_acidente",
        "classificacao_acidente", "fase_dia", "sentido_via", "condicao_metereologica",
        "tipo_pista", "tracado_via", "uso_solo"
    ]

    for c in colunas_texto:
        df_silver = df_silver.withColumn(c, upper(trim(col(c))))
        df_silver = df_silver.withColumn(c, when(col(c).isin("NULL", "NULO", "(N/A)", "NAO INFORMADO"), None).otherwise(col(c)))

    # Criar coluna 'data_hora_acidente' (Timestamp)
    df_silver = df_silver.withColumn(
    "data_hora_acidente",
    to_timestamp(concat_ws(" ", col("data_inversa"), col("horario")), "dd/MM/yyyy HH:mm:ss")
)
    
    # Criar coluna 'ano' para particionamento
    df_silver = df_silver.withColumn("ano", year(col("data_hora_acidente")))

    # 6. Salvar os dados limpos na camada Silver (em formato Parquet)
    
    print("Iniciando escrita para a camada Silver...")
    
    (
        df_silver
        .write
        .mode("overwrite") # Sobrescreve os dados se já existirem
        .format("parquet")
        .partitionBy("ano", "uf") # Particiona os dados para performance
        .save(SILVER_PATH)
    )

    print("Processo concluído! Dados salvos na camada Silver.")
    spark.stop()

# Padrão Python para executar o script
if __name__ == "__main__":
    main()