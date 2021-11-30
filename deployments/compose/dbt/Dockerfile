FROM fishtownanalytics/dbt:0.21.1rc2

WORKDIR /usr/app
RUN apt update
RUN apt install  libsasl2-dev python3-dev -y
RUN pip install "dbt-spark[PyHive]"
RUN pip install dbt-trino
