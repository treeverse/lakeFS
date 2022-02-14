FROM jupyter/pyspark-notebook:notebook-6.4.6

ENV HADOOP_VERSION=3.3.1
ENV AWS_SDK_VERSION=1.11.901
ENV SPARK_HOME=/usr/local/spark-3.2.0-bin-hadoop3.2

USER root

RUN apt update -y && apt install -y curl
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_VERSION}.jar

USER $NB_UID
