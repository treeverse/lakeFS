# INSTRUCTIONS
#
# Build:
#     docker build --tag treeverse/bitnami-based-spark:3.2.3-with-hadoop-2.7-for-testing .
#
# Push:
#     docker push treeverse/bitnami-based-spark:3.2.3-with-hadoop-2.7-for-testing
#
# Use *only* in Esti.  This is **NOT** a good Docker image to use for
# running Spark, except for testing.

FROM --platform=$BUILDPLATFORM ubuntu:21.04 AS extract

WORKDIR /build
# Extract hadoop-aws-2.7.4 and its dependency aws-java-sdk-1.7.4 from an old
# archived version of Apachehe Hadoop.  These JARs are so long-dead that
# this is the *easy* way of getting our actual test code onthem.
#
# See, fear, and never use this Docker image except in tests.
ADD https://archive.apache.org/dist/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz /tmp/hadoop-2.7.4.tar.gz
RUN tar --extract --to-stdout --gzip --strip 5 --file /tmp/hadoop-2.7.4.tar.gz hadoop-2.7.4/share/hadoop/tools/lib/hadoop-aws-2.7.4.jar > ./hadoop-aws-2.7.4.jar
RUN tar --extract --to-stdout --gzip --strip 5 --file /tmp/hadoop-2.7.4.tar.gz hadoop-2.7.4/share/hadoop/tools/lib/aws-java-sdk-1.7.4.jar > ./aws-java-sdk-1.7.4.jar

# Build Bitnami Spark 3.2.x but with Hadoop 2.  Details in
# https://github.com/bitnami/bitnami-docker-spark#using-a-different-version-of-hadoop-jars.
FROM bitnami/spark:3.2.3

USER root
RUN rm -r /opt/bitnami/spark/jars
ADD https://dlcdn.apache.org/spark/spark-3.2.3/spark-3.2.3-bin-hadoop2.7.tgz /tmp/
RUN tar --extract --gzip --strip=1 < /tmp/spark-3.2.3-bin-hadoop2.7.tgz --directory /opt/bitnami/spark/ spark-3.2.3-bin-hadoop2.7/jars/ && rm /tmp/spark-3.2.3-bin-hadoop2.7.tgz
COPY --from=extract /build/*.jar /opt/bitnami/spark/jars/
USER 1001
