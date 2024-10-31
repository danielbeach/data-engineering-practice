FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y default-jdk scala wget vim software-properties-common python3.8 python3-pip curl unzip libpq-dev build-essential libssl-dev libffi-dev python3-dev && \
    apt-get clean

RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
tar xvf spark-3.5.0-bin-hadoop3.tgz && \
mv spark-3.5.0-bin-hadoop3/ /usr/local/spark && \
ln -s /usr/local/spark spark

WORKDIR app
COPY . /app

RUN pip3 install -r requirements.txt --break-system-packages

ENV PYSPARK_PYTHON=python3