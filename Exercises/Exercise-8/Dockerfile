FROM python:3.11

#RUN apt-get update && \
  #  apt-get install -y software-properties-common && \
  #  add-apt-repository ppa:deadsnakes/ppa && \
#    apt-get update && \
#    apt-get install -y vim software-properties-common python3 python3-pip libpq-dev build-essential libssl-dev libffi-dev python3-dev && \
#    apt-get clean

WORKDIR app
COPY . /app

RUN pip3 install -r requirements.txt
