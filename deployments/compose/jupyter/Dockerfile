FROM jupyter/pyspark-notebook:notebook-6.4.6

ENV HADOOP_VERSION=3.3.1
ENV AWS_SDK_VERSION=1.11.901
ENV SPARK_HOME=/usr/local/spark-3.2.0-bin-hadoop3.2

USER root

RUN apt update -y && apt install -y curl
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar -o /usr/local/spark-3.2.0-bin-hadoop3.2/jars/aws-java-sdk-bundle-1.11.901.jar
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar -o /usr/local/spark-3.2.0-bin-hadoop3.2/jars/hadoop-aws-3.3.1.jar
