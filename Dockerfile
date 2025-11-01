# 1. Partimos da imagem oficial do Airflow
FROM apache/airflow:2.7.3

# --- NOSSAS NOVAS INSTRUÇÕES ---
# 2. Mudamos para o usuário 'root' para poder instalar pacotes
USER root

# 3. Atualizamos o 'apt' e instalamos o Java (JDK) e o 'procps'
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless \
    procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 4. Definimos a variável de ambiente JAVA_HOME que o Spark procura
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
# --- FIM DAS NOVAS INSTRUÇÕES ---

# 5. Voltamos para o usuário 'airflow'
USER airflow

# 6. Instalamos a biblioteca do PySpark (como antes)
RUN pip install pyspark==3.5.0