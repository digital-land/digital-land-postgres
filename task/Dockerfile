FROM ubuntu:20.04

RUN apt-get update \
    && apt-get install -y curl sqlite python3-pip unzip \
    && apt-get clean

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip -q awscliv2.zip \
    && ./aws/install --update

ADD . /src
WORKDIR /src

RUN pip install --user -U pip
RUN pip install --user --no-cache-dir -r requirements.txt

ENTRYPOINT [ "./load.sh" ]
