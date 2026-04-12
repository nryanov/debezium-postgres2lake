FROM quay.io/jupyter/pyspark-notebook:spark-4.1.1

ENV AWS_SDK_VERSION=2.29.52
ENV HADOOP_VERSION=3.4.2

RUN mamba install --yes 'psycopg[binary]' && \
    mamba install --yes 'pyiceberg[pyarrow,sql-postgres,hive]' && \
    mamba clean --all -f -y && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

USER root

RUN curl -fsSL -o "/usr/local/spark/jars/postgresql-42.7.10.jar" "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.10/postgresql-42.7.10.jar" && \
    curl -fsSL -o "/usr/local/spark/jars/spark-avro_2.13-4.1.1.jar" "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/4.1.1/spark-avro_2.13-4.1.1.jar" && \
    curl -fsSL -o "/usr/local/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar" "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" && \
    curl -fsSL -o "/usr/local/spark/jars/aws-core-2.29.52.jar" "https://repo1.maven.org/maven2/software/amazon/awssdk/aws-core/2.29.52/aws-core-2.29.52.jar" && \
    curl -fsSL -o "/usr/local/spark/jars/bundle-${AWS_SDK_VERSION}.jar" "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar"

USER jovyan

RUN mkdir /home/jovyan/extra-jars
RUN curl -fsSL -o "/home/jovyan/extra-jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar" "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.1/iceberg-spark-runtime-4.0_2.13-1.10.1.jar" && \
    curl -fsSL -o "/home/jovyan/extra-jars/paimon-spark-4.0-1.3.1.jar" "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-4.0/1.3.1/paimon-spark-4.0-1.3.1.jar"
