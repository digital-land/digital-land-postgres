FROM ubuntu:20.04

RUN apt update
RUN apt install -y curl sqlite python3-pip
RUN curl https://packages.cloudfoundry.org/debian/cli.cloudfoundry.org.key | apt-key add -
RUN echo "deb https://packages.cloudfoundry.org/debian stable main" | tee /etc/apt/sources.list.d/cloudfoundry-cli.list
RUN apt update
RUN apt install -y cf8-cli
RUN cf install-plugin -f conduit
RUN apt clean

ADD . /src
WORKDIR /src

RUN pip install --user -U pip
RUN pip install --user --no-cache-dir -r requirements.txt

ENTRYPOINT [ "./load.sh" ]
