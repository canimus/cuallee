FROM python:3.11

RUN apt update -y
RUN apt install wget -y
RUN apt install openjdk-17-jdk -y

WORKDIR /usr/src
ENV PIP_ROOT_USER_ACTION=ignore
RUN pip install pyspark==3.5.1
RUN pip install -i https://pypi.cloud.soda.io soda-spark-df==1.4.10

RUN mkdir -p /data
RUN wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet -P /data
RUN wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet -P /data

ENV SODA_KEY=
ENV SODA_SECRET=

RUN rm -rf /root/.cache/pip
