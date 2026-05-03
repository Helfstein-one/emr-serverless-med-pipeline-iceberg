# ============================================================
# Spark 3.5.3 + Iceberg + Nessie + Hadoop-AWS
# Imagem oficial Apache Spark com PySpark
# ============================================================
FROM docker.io/apache/spark:3.5.3-python3

USER root

# --- Versões dos JARs ---
ENV ICEBERG_VERSION=1.7.1
ENV NESSIE_VERSION=0.106.0
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.367
ENV SCALA_VERSION=2.12
ENV SPARK_JARS_DIR=/opt/spark/jars

# --- Download dos JARs necessários ---
RUN curl -fSL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
      -o ${SPARK_JARS_DIR}/iceberg-spark-runtime.jar && \
    curl -fSL "https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_${SCALA_VERSION}/${NESSIE_VERSION}/nessie-spark-extensions-3.5_${SCALA_VERSION}-${NESSIE_VERSION}.jar" \
      -o ${SPARK_JARS_DIR}/nessie-spark-extensions.jar && \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      -o ${SPARK_JARS_DIR}/hadoop-aws.jar && \
    curl -fSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle.jar && \
    curl -fSL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/${ICEBERG_VERSION}/iceberg-aws-${ICEBERG_VERSION}.jar" \
      -o ${SPARK_JARS_DIR}/iceberg-aws.jar && \
    curl -fSL "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.28.3/bundle-2.28.3.jar" \
      -o ${SPARK_JARS_DIR}/aws-sdk-v2-bundle.jar && \
    curl -fSL "https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.28.3/url-connection-client-2.28.3.jar" \
      -o ${SPARK_JARS_DIR}/url-connection-client.jar

# --- Copia configurações do Spark ---
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# --- Instala dependências Python ---
RUN pip install --no-cache-dir boto3

# --- Cria diretórios para os jobs ---
RUN mkdir -p /app/spark/jobs /app/spark/utils /app/data

# --- Expõe portas ---
EXPOSE 10000 4040 7077

USER spark

# --- Container fica vivo aguardando comandos ---
# O Thrift Server será iniciado sob demanda pelo pipeline.sh
# para não competir memória com os spark-submit jobs.
ENTRYPOINT ["tail", "-f", "/dev/null"]
