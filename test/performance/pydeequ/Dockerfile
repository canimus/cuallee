FROM python:3.8.10

RUN apt update -y
RUN apt install wget -y

WORKDIR /usr/src
RUN wget https://repo1.maven.org/maven2/com/amazon/deequ/deequ/2.0.1-spark-3.2/deequ-2.0.1-spark-3.2.jar

RUN apt install openjdk-11-jdk -y
RUN pip install pyspark==3.3.0
RUN pip install pydeequ==1.0.1
RUN pip install cuallee==0.1.2
RUN pip install colorama
RUN pip install pyarrow
RUN pip install fastparquet
RUN pip install toolz

COPY sparker.py .
COPY sparker2.py .

RUN mkdir -p /usr/src/data
VOLUME /usr/src/data
ENV SPARK_VERSION="3.3.0"
# ENTRYPOINT ["python", "/usr/src/sparker2.py"]
