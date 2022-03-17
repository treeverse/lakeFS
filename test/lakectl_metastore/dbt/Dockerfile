FROM ubuntu:20.04

WORKDIR /usr/app
RUN apt update
RUN apt install  libsasl2-dev python3-dev python3-pip git curl jq -y
COPY requirements.txt ./
RUN pip install -r requirements.txt