FROM jupyter/pyspark-notebook:notebook-6.5.4

ENV HADOOP_AWS_VERSION=3.3.1
ENV AWS_SDK_VERSION=1.11.901

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl \
      unzip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -O ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -O ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar

USER $NB_UID

# Install python deps including lakeFS client
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Set up iPython pretty tables and Spark SQL magic
RUN mkdir -p /home/jovyan/.ipython/profile_default/startup
COPY ipython/startup/00-prettytables.py /home/jovyan/.ipython/profile_default/startup
COPY ipython/startup/README /home/jovyan/.ipython/profile_default/startup

# Download some sample data
RUN mkdir -p /home/jovyan/data \
 && curl https://data.cityofnewyork.us/resource/tg4x-b46p.json > /home/jovyan/data/nyc_film_permits.json


ENV NOTEBOOK_ARGS="--NotebookApp.token='' --NotebookApp.password=''"