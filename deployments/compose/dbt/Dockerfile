FROM ubuntu:20.04

WORKDIR /usr/app
RUN apt update
RUN apt install  libsasl2-dev python3-dev python3-pip git -y
RUN pip install "dbt-spark[PyHive]"
RUN pip install dbt-trino
