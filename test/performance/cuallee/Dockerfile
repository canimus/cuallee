FROM python:3.8.10

RUN apt update -y
RUN apt install wget -y

WORKDIR /usr/src

RUN apt install openjdk-11-jdk -y
RUN pip install pyspark==3.3.0
RUN pip install cuallee==0.0.9

COPY sparker.py .

RUN mkdir -p /usr/src/data
VOLUME /usr/src/data

ENTRYPOINT ["python", "/usr/src/sparker.py"]
