from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define o comando base para executar nossos scripts PySpark
# Vamos usar o 'spark-submit' para enviar o job ao nosso cluster Spark
# '--master spark://spark-master:7077' -> Aponta para o nosso Master do Spark
# '--packages ...' -> Pede ao Spark para baixar os "drivers" para falar com S3 (MinIO) e PostgreSQL
# '{{ params.script_path }}' -> É uma variável do Airflow; vamos definir o caminho do script em cada tarefa
spark_submit_command = """
    spark-submit --master spark://spark-master:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.5.0 \
    {{ params.script_path }}
"""

with DAG(
    dag_id="pipeline_acidentes_brasil", # Nome da DAG no Airflow
    start_date=datetime(2025, 1, 1),    # Data de início (pode ser qualquer data no passado)
    schedule_interval=None,           # 'None' significa que só vamos rodar manualmente
    catchup=False,                      # Não tenta rodar execuções passadas
    tags=["projeto", "spark", "etl", "acidentes"] # Etiquetas para organizar
) as dag:

    # Tarefa 1: Executar o script Bronze -> Silver
    task_bronze_to_silver = BashOperator(
        task_id="processamento_bronze_para_silver",
        bash_command=spark_submit_command,
        params={
            "script_path": "/opt/spark/jobs/bronze_to_silver.py"
        }
    )

    # Tarefa 2: Executar o script Silver -> Gold
    task_silver_to_gold = BashOperator(
        task_id="processamento_silver_para_gold",
        bash_command=spark_submit_command,
        params={
            "script_path": "/opt/spark/jobs/silver_to_gold.py"
        }
    )

    # --- Definindo a Ordem (A parte mais importante!) ---
    # Esta linha diz: "Execute task_bronze_to_silver e, SOMENTE SE ela
    # for bem-sucedida, execute task_silver_to_gold."
    
    task_bronze_to_silver >> task_silver_to_gold