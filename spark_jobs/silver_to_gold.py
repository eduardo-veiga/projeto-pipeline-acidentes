import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, dayofweek, hour, 
    monotonically_increasing_id
)

def main():
    print("Iniciando processo de Silver para Gold...")

    # 1. Criar a SparkSession
    spark = (
        SparkSession.builder.appName("ProcessamentoSilverParaGold")
        .master("local[*]")
        .getOrCreate()
    )

    # 2. Configurar o Spark para falar com o MinIO (S3) - para LEITURA
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # 3. Definir informações de conexão do Data Warehouse (PostgreSQL)
    JDBC_URL = "jdbc:postgresql://postgres-dw:5432/acidentes_dw"
    JDBC_PROPERTIES = {
        "user": "admin_dw",
        "password": "admin_dw",
        "driver": "org.postgresql.Driver"
    }

    # 4. Definir caminhos de Leitura (Silver)
    SILVER_PATH = "s3a://silver/acidentes_limpos"
    
    # 5. Ler os dados limpos (Parquet) da camada Silver
    print("Lendo dados limpos da camada Silver...")
    df_silver = spark.read.parquet(SILVER_PATH)

    # 6. Criar o Modelo Dimensional (Star Schema)

    # --- dim_tempo ---
    print("Criando dim_tempo...")
    df_dim_tempo = (
        df_silver.select("data_hora_acidente", "fase_dia")
        .distinct()
        .withColumn("id_tempo", monotonically_increasing_id())
        .select(
            col("id_tempo"),
            col("data_hora_acidente"),
            year(col("data_hora_acidente")).alias("ano"),
            month(col("data_hora_acidente")).alias("mes"),
            dayofmonth(col("data_hora_acidente")).alias("dia"),
            dayofweek(col("data_hora_acidente")).alias("dia_semana"),
            hour(col("data_hora_acidente")).alias("hora"),
            col("fase_dia")
        )
    )

    # --- dim_localizacao ---
    print("Criando dim_localizacao...")
    df_dim_localizacao = (
        df_silver.select("uf", "br", "km", "municipio", "regional", "delegacia", "uop", "latitude", "longitude")
        .distinct()
        .withColumn("id_localizacao", monotonically_increasing_id())
    )

    # --- dim_causa ---
    print("Criando dim_causa...")
    df_dim_causa = (
        df_silver.select("causa_acidente", "tipo_acidente", "classificacao_acidente")
        .distinct()
        .withColumn("id_causa", monotonically_increasing_id())
    )
    
    # --- dim_condicao_via ---
    print("Criando dim_condicao_via...")
    df_dim_condicao_via = (
        df_silver.select("condicao_metereologica", "tipo_pista", "tracado_via", "sentido_via", "uso_solo")
        .distinct()
        .withColumn("id_condicao_via", monotonically_increasing_id())
    )

    # --- fato_acidentes ---
    print("Criando fato_acidentes...")

    # Lista das colunas de join para cada dimensão
    join_cols_tempo = ["data_hora_acidente", "fase_dia"]
    join_cols_localizacao = ["uf", "br", "km", "municipio", "regional", "delegacia", "uop", "latitude", "longitude"]
    join_cols_causa = ["causa_acidente", "tipo_acidente", "classificacao_acidente"]
    join_cols_condicao = ["condicao_metereologica", "tipo_pista", "tracado_via", "sentido_via", "uso_solo"]

    # Criar condições de join "Null-Safe" (usando .eqNullSafe())
    join_expr_tempo = [df_silver[c].eqNullSafe(df_dim_tempo[c]) for c in join_cols_tempo]
    join_expr_localizacao = [df_silver[c].eqNullSafe(df_dim_localizacao[c]) for c in join_cols_localizacao]
    join_expr_causa = [df_silver[c].eqNullSafe(df_dim_causa[c]) for c in join_cols_causa]
    join_expr_condicao = [df_silver[c].eqNullSafe(df_dim_condicao_via[c]) for c in join_cols_condicao]

    df_fato = (
            df_silver
            .join(df_dim_tempo, on=join_expr_tempo, how="left")
            .join(df_dim_localizacao, on=join_expr_localizacao, how="left")
            .join(df_dim_causa, on=join_expr_causa, how="left")
            .join(df_dim_condicao_via, on=join_expr_condicao, how="left")
            # Bloco NOVO (Corrigido)
            .select(
                df_silver["id"].alias("id_acidente_original"),
                df_dim_tempo["id_tempo"],
                df_dim_localizacao["id_localizacao"],
                df_dim_causa["id_causa"],
                df_dim_condicao_via["id_condicao_via"],
                df_silver["pessoas"],
                df_silver["mortos"],
                df_silver["feridos_leves"],
                df_silver["feridos_graves"],
                df_silver["ilesos"],
                df_silver["ignorados"],
                df_silver["feridos"],
                df_silver["veiculos"]
            )
        )

    # 7. Salvar as tabelas no Data Warehouse (PostgreSQL)
    print("Iniciando escrita no PostgreSQL...")

    # Escrever dimensões
    (
        df_dim_tempo
        .write
        .jdbc(url=JDBC_URL, table="dim_tempo", mode="overwrite", properties=JDBC_PROPERTIES)
    )
    (
        df_dim_localizacao
        .write
        .jdbc(url=JDBC_URL, table="dim_localizacao", mode="overwrite", properties=JDBC_PROPERTIES)
    )
    (
        df_dim_causa
        .write
        .jdbc(url=JDBC_URL, table="dim_causa", mode="overwrite", properties=JDBC_PROPERTIES)
    )
    (
        df_dim_condicao_via
        .write
        .jdbc(url=JDBC_URL, table="dim_condicao_via", mode="overwrite", properties=JDBC_PROPERTIES)
    )
    
    # Escrever fato
    (
        df_fato
        .write
        .jdbc(url=JDBC_URL, table="fato_acidentes", mode="overwrite", properties=JDBC_PROPERTIES)
    )

    print("Processo Gold concluído! Tabelas salvas no Data Warehouse.")
    spark.stop()

# Padrão Python para executar o script
if __name__ == "__main__":
    main()