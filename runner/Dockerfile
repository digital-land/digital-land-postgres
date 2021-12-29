FROM ubuntu:18.04

RUN apt-get update
RUN apt-get install -y postgresql-client curl sqlite python-psycopg2
RUN apt-get clean

ADD . /src
WORKDIR src

CMD ./fetch_and_load_data.sh