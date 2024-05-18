FROM python:3.11

RUN apt update -y
RUN apt install wget -y
RUN apt install openjdk-17-jdk -y

WORKDIR /usr/src
ENV PIP_ROOT_USER_ACTION=ignore
RUN pip install pyspark==3.5.1
RUN pip install pydeequ==1.3.0

# Required to prevent an warning message on initialization
ENV SPARK_VERSION="3.3"

RUN mkdir -p /data
RUN wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet -P /data
RUN wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet -P /data

RUN mkdir -p /libs
RUN wget https://repo1.maven.org/maven2/com/amazon/deequ/deequ/2.0.7-spark-3.5/deequ-2.0.7-spark-3.5.jar -P /libs

RUN rm -rf /root/.cache/pip