FROM ubuntu:20.04

RUN apt update
RUN apt install -y curl sqlite python3-pip python3-psycopg2
RUN apt clean

ADD . /src
WORKDIR src

RUN pip install --user -U pip
RUN pip install --user --no-cache-dir -r requirements.txt
