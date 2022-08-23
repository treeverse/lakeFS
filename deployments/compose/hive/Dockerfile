FROM openjdk:8u242-jre-buster

WORKDIR /opt

ENV HADOOP_VERSION=3.2.0
ENV HIVE_VERSION=3.1.2
ENV AWS_SDK_VERSION=1.11.375
ENV HADOOP_HOME=/opt/hadoop
ENV HIVE_HOME=/opt/apache-hive-bin

RUN mkdir ${HADOOP_HOME} ${HIVE_HOME}
RUN curl -sSL https://www.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz | tar -zx --strip-components 1 -C ${HIVE_HOME} -f -
RUN curl -sSL https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar -zx --strip-components 1 -C ${HADOOP_HOME} -f -
RUN curl -sSL https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.19.tar.gz | tar -zx --strip-components 1 -C ${HIVE_HOME}/lib -f - mysql-connector-java-8.0.19/mysql-connector-java-8.0.19.jar
RUN ln -s ${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar ${HIVE_HOME}/lib/
RUN ln -s ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar ${HIVE_HOME}/lib/
RUN apt-get update -y && apt install -y netcat procps wait-for-it mariadb-client

COPY entrypoint.sh /

ENV HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar
ENV PATH=${HIVE_HOME}/bin:${PATH}

ENTRYPOINT "/entrypoint.sh"
