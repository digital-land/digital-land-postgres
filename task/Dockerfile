FROM python:3.9-slim-bookworm

RUN apt-get update \
    && apt-get install -y curl sqlite3 unzip git \
    && apt-get clean

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip -q awscliv2.zip \
    && ./aws/install --update

ADD . /src
WORKDIR /src

RUN python3 --version
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT [ "./load.sh" ]
